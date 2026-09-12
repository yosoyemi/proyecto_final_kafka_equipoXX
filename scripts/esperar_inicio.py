"""Espera datos nuevos de los cuatro topics y una respuesta del dashboard."""

import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHIVOS = [ROOT / "datos" / f"{t}.csv" for t in ("ventas", "pagos", "clientes", "inventario")]


def tamano(path):
    try:
        return path.stat().st_size
    except OSError:
        return 0


def main():
    iniciales = {path: tamano(path) for path in ARCHIVOS}
    pendientes = set(ARCHIVOS)
    limite = time.monotonic() + 60
    http_listo = False
    while time.monotonic() < limite:
        pendientes = {p for p in pendientes if tamano(p) <= iniciales[p]}
        try:
            with urllib.request.urlopen("http://127.0.0.1:8501/_stcore/health", timeout=2) as response:
                http_listo = response.status == 200 and response.read().strip() == b"ok"
        except (OSError, urllib.error.URLError):
            http_listo = False
        if not pendientes and http_listo:
            print("[OK] Dashboard disponible y los cuatro CSV reciben datos nuevos.", flush=True)
            return 0
        time.sleep(1)
    if pendientes:
        print("[ERROR] No llegaron datos nuevos a: " + ", ".join(sorted(p.name for p in pendientes)))
    if not http_listo:
        print("[ERROR] El dashboard no responde en el puerto 8501.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
