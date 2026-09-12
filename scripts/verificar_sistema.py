"""Verifica que el sistema Obelie Kafka este completo y funcionando."""

from __future__ import annotations

import csv
import socket
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from catalogo import BOOTSTRAP_SERVERS, TOPICS, errores_sin_broker

DATOS = ROOT / "datos"
README = ROOT / "README.txt"
DASHBOARD = ROOT / "dashboard" / "dashboard.py"
ERRORES: list[str] = []


def ok(mensaje: str) -> None:
    print(f"[OK] {mensaje}")


def fail(mensaje: str) -> None:
    print(f"[FAIL] {mensaje}")
    ERRORES.append(mensaje)


def command_lines_python() -> list[str]:
    comando = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -match 'python|streamlit' } | "
        "Select-Object -ExpandProperty CommandLine"
    )
    try:
        resultado = subprocess.run(
            ["powershell", "-NoProfile", "-Command", comando],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    lineas = []
    for linea in (resultado.stdout or "").splitlines():
        texto = linea.strip()
        if texto:
            lineas.append(texto)
    return lineas


def kafka_admin():
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id="obelie-verificar",
            request_timeout_ms=8000,
        )
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            consumer_timeout_ms=1000,
            request_timeout_ms=8000,
        )
        return admin, consumer
    except errores_sin_broker() as exc:
        fail(f"Kafka no responde en {BOOTSTRAP_SERVERS}: {exc}")
        return None, None
    except Exception as exc:
        fail(f"Error conectando a Kafka: {exc}")
        return None, None


def verificar_puerto() -> None:
    host, _, puerto = BOOTSTRAP_SERVERS.partition(":")
    try:
        with socket.create_connection((host, int(puerto)), timeout=5):
            ok(f"Puerto Kafka abierto ({BOOTSTRAP_SERVERS})")
    except OSError:
        fail(f"No hay servicio en {BOOTSTRAP_SERVERS}")


def verificar_topics() -> None:
    admin, consumer = kafka_admin()
    if admin is None or consumer is None:
        return
    try:
        existentes = set(admin.list_topics())
        for topic in TOPICS:
            if topic not in existentes:
                fail(f"Falta topic '{topic}'")
            else:
                ok(f"Topic '{topic}' existe")
        for topic in TOPICS:
            partes = consumer.partitions_for_topic(topic)
            if not partes:
                fail(f"No se pudieron leer particiones de '{topic}'")
                continue
            if len(partes) != 3:
                fail(f"Topic '{topic}' tiene {len(partes)} particiones (se esperaban 3)")
            else:
                ok(f"Topic '{topic}' tiene 3 particiones")
    finally:
        try:
            admin.close()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass


def verificar_procesos() -> None:
    lineas = command_lines_python()
    texto = "\n".join(lineas).lower()
    for nombre in ("producer_1.py", "producer_2.py", "producer_3.py"):
        if nombre in texto:
            ok(f"Proceso activo: {nombre}")
        else:
            fail(f"No se encontro el proceso {nombre}")
    if "consumer.py" in texto:
        ok("Proceso activo: consumer.py")
    else:
        fail("No se encontro el proceso consumer.py")
    if "dashboard.py" in texto or "streamlit" in texto:
        ok("Proceso activo: dashboard Streamlit")
    else:
        fail("No se encontro el proceso del dashboard Streamlit")


def contar_filas(path: Path) -> int:
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        filas = list(reader)
    if not filas:
        return 0
    return max(0, len(filas) - 1)


def verificar_csv() -> None:
    for topic in TOPICS:
        path = DATOS / f"{topic}.csv"
        if not path.exists():
            fail(f"No existe {path.relative_to(ROOT)}")
            continue
        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)
                filas = list(reader)
        except OSError as exc:
            fail(f"No se pudo leer {path.name}: {exc}")
            continue
        if not filas:
            fail(f"{path.name} esta vacio (sin encabezados)")
            continue
        encabezado = filas[0]
        if not encabezado or all(not c.strip() for c in encabezado):
            fail(f"{path.name} no tiene encabezados")
        else:
            ok(f"{path.name} tiene encabezados: {', '.join(encabezado)}")
        registros = max(0, len(filas) - 1)
        if registros < 20:
            fail(f"{path.name} tiene {registros} registros (minimo 20)")
        else:
            ok(f"{path.name} tiene {registros} registros")


def verificar_dashboard_http() -> None:
    url = "http://localhost:8501/_stcore/health"
    try:
        with urllib.request.urlopen(url, timeout=5) as respuesta:
            cuerpo = respuesta.read().decode("utf-8", errors="ignore").strip().lower()
            if respuesta.status == 200:
                ok(f"Dashboard Streamlit responde en http://localhost:8501 ({cuerpo or 'ok'})")
            else:
                fail(f"Dashboard HTTP status {respuesta.status}")
    except urllib.error.URLError as exc:
        fail(f"Dashboard no responde en http://localhost:8501: {exc}")
    except Exception as exc:
        fail(f"Dashboard no verificado: {exc}")

    if DASHBOARD.exists():
        ok("Archivo dashboard/dashboard.py presente")
    else:
        fail("Falta dashboard/dashboard.py")


def verificar_supabase() -> None:
    try:
        from obelie_supabase import ping_supabase
    except Exception as exc:
        fail(f"No se pudo importar el cliente de Supabase: {exc}")
        return
    ok_conexion, detalle = ping_supabase()
    if ok_conexion:
        ok(detalle)
    else:
        fail(f"Supabase no responde: {detalle}")


def verificar_readme() -> None:
    if not README.exists():
        fail("Falta README.txt")
        return
    texto = README.read_text(encoding="utf-8", errors="ignore")
    if len(texto.strip()) < 200:
        fail("README.txt esta incompleto")
        return
    pistas = ["kafka", "producer", "consumer", "streamlit", "topic"]
    faltan = [p for p in pistas if p not in texto.lower()]
    if faltan:
        fail(f"README.txt no explica: {', '.join(faltan)}")
    else:
        ok("README.txt presente y con instrucciones")


def main() -> None:
    print("Verificando sistema Obelie Kafka...")
    print("-" * 50)
    verificar_puerto()
    verificar_topics()
    verificar_procesos()
    verificar_csv()
    verificar_dashboard_http()
    verificar_supabase()
    verificar_readme()
    print("-" * 50)
    if ERRORES:
        print("VERIFICACION FALLIDA:")
        for item in ERRORES:
            print(f"  - {item}")
        sys.exit(1)
    print("PROYECTO LISTO PARA ENTREGAR")


if __name__ == "__main__":
    main()
