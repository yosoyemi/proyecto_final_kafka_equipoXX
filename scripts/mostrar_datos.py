"""Imprime en la terminal el POS de Supabase; usa Kafka solo si no hay conexion."""

from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DATOS = ROOT / "datos"
ARCHIVOS = {
    "ventas": DATOS / "ventas.csv",
    "pagos": DATOS / "pagos.csv",
    "clientes": DATOS / "clientes.csv",
    "inventario": DATOS / "inventario.csv",
}


def _leer_filas(path: Path) -> list[dict[str, str]]:
    try:
        texto = path.read_text(encoding="utf-8-sig")
    except OSError:
        return []
    lineas = [ln for ln in texto.splitlines() if ln.strip()]
    if len(lineas) < 2:
        return []
    encabezados = [c.strip() for c in lineas[0].split(",")]
    filas = []
    for linea in lineas[1:]:
        valores = [c.strip() for c in linea.split(",")]
        filas.append(dict(zip(encabezados, valores)))
    return filas


def _hora(valor) -> str:
    texto = str(valor or "").replace("T", " ")
    if len(texto) >= 19:
        return texto[11:19]
    return texto


def _linea_venta(origen: str, fila: dict) -> str:
    producto = fila.get("producto") or "Producto"
    try:
        cantidad = f"{float(fila.get('cantidad') or 1):g}"
    except (TypeError, ValueError):
        cantidad = str(fila.get("cantidad") or "1")
    try:
        total = f"{float(fila.get('total') or 0):.2f}"
    except (TypeError, ValueError):
        total = str(fila.get("total") or "0")
    sucursal = fila.get("sucursal") or ""
    fecha = _hora(fila.get("fecha_hora") or fila.get("fecha_venta") or "")
    return f"[{origen:<6}] {fecha}  {sucursal:<14} {producto}  x{cantidad}  ${total}"


def _clave_venta(fila: dict) -> str:
    return (
        str(fila.get("id_venta") or "")
        + "|"
        + str(fila.get("producto") or "")
        + "|"
        + str(fila.get("fecha_hora") or "")
    )


def _imprimir_pos() -> bool:
    try:
        from obelie_supabase import cargar_panel_supabase, ping_supabase
    except Exception as exc:
        print(f"[POS] No se pudo importar Supabase: {exc}", flush=True)
        return False
    ok, mensaje = ping_supabase()
    print(f"[POS] {mensaje}", flush=True)
    if not ok:
        return False
    try:
        panel = cargar_panel_supabase()
    except Exception as exc:
        print(f"[POS] Error al consultar: {exc}", flush=True)
        return False
    ventas = panel.get("ventas")
    n = 0 if ventas is None or ventas.empty else len(ventas)
    tickets = panel.get("n_tickets") or 0
    unidades = panel.get("n_unidades") or 0
    print(
        f"[POS] {tickets} tickets · {n} lineas · {unidades:.0f} unidades · "
        f"{panel.get('n_clientes') or 0} clientes",
        flush=True,
    )
    return True


def _monitor_pos() -> int:
    vistos: set[str] = set()
    inicial = True
    while True:
        try:
            from obelie_supabase import cargar_panel_supabase

            panel = cargar_panel_supabase()
        except Exception as exc:
            print(f"[POS] Error al consultar: {exc}", flush=True)
            return 1
        ventas = panel.get("ventas")
        if ventas is None or ventas.empty:
            time.sleep(3)
            continue
        ordenadas = ventas.sort_values("fecha_hora", kind="stable")
        if inicial:
            for _, fila in ordenadas.tail(8).iterrows():
                print(_linea_venta("POS", fila.to_dict()), flush=True)
            vistos = {_clave_venta(fila.to_dict()) for _, fila in ordenadas.iterrows()}
            inicial = False
        else:
            for _, fila in ordenadas.iterrows():
                dato = fila.to_dict()
                clave = _clave_venta(dato)
                if clave in vistos:
                    continue
                vistos.add(clave)
                print(_linea_venta("POS", dato), flush=True)
        time.sleep(3)


def _monitor_kafka() -> int:
    print("--- Respaldo: streaming Kafka (CSV) ---", flush=True)
    vistos = {nombre: 0 for nombre in ARCHIVOS}
    while True:
        for nombre, path in ARCHIVOS.items():
            filas = _leer_filas(path)
            previo = vistos[nombre]
            if len(filas) <= previo:
                if len(filas) < previo:
                    vistos[nombre] = len(filas)
                continue
            nuevas = filas[previo:]
            vistos[nombre] = len(filas)
            for fila in nuevas:
                if nombre == "ventas":
                    print(_linea_venta("Kafka", fila), flush=True)
                elif nombre == "pagos":
                    print(
                        f"[Kafka] pago  {_hora(fila.get('fecha_hora'))}  "
                        f"{fila.get('metodo_pago') or ''}  ${fila.get('monto') or '0'}  "
                        f"{fila.get('estado') or ''}",
                        flush=True,
                    )
                elif nombre == "clientes":
                    print(
                        f"[Kafka] cliente  {fila.get('nombre') or fila.get('id_cliente') or ''}  "
                        f"{fila.get('ciudad') or ''}  {fila.get('sucursal_preferida') or ''}",
                        flush=True,
                    )
                else:
                    print(
                        f"[Kafka] stock  {fila.get('sucursal') or ''}  "
                        f"{fila.get('producto') or ''}  {fila.get('stock') or ''} pzas",
                        flush=True,
                    )
        time.sleep(0.6)


def main() -> int:
    print("", flush=True)
    print("=== Datos en vivo del POS (Supabase) ===", flush=True)
    print("Ctrl+C detiene este monitor. El sistema sigue activo. Usa DETENER.bat para apagarlo.", flush=True)
    print("", flush=True)
    conectado = _imprimir_pos()
    try:
        if conectado:
            print("", flush=True)
            print("--- POS en vivo ---", flush=True)
            return _monitor_pos()
        print("", flush=True)
        print("Sin conexion a Supabase. Se usa el streaming de Kafka.", flush=True)
        return _monitor_kafka()
    except KeyboardInterrupt:
        print("\nMonitor detenido.", flush=True)
        return 0


if __name__ == "__main__":
    sys.exit(main())
