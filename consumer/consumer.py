"""Consumer unico: escucha los 4 topics y escribe/append CSV en datos/."""

from __future__ import annotations

import csv
import json
import os
import sys
import time
from pathlib import Path

from kafka import KafkaConsumer

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from catalogo import BOOTSTRAP_SERVERS, TOPICS, errores_sin_broker

DATOS = ROOT / "datos"

CAMPOS = {
    "ventas": [
        "id_venta",
        "fecha_hora",
        "producto",
        "cantidad",
        "precio_unitario",
        "total",
        "sucursal",
        "vendedor",
    ],
    "clientes": [
        "id_cliente",
        "fecha_hora",
        "nombre",
        "email",
        "telefono",
        "ciudad",
        "tipo_cliente",
        "sucursal_preferida",
    ],
    "pagos": [
        "id_pago",
        "id_venta",
        "fecha_hora",
        "metodo_pago",
        "monto",
        "estado",
        "referencia",
    ],
    "inventario": [
        "id_evento",
        "fecha_hora",
        "producto",
        "stock",
        "sucursal",
        "categoria",
        "unidad",
        "costo_unitario",
    ],
}

CONTADORES = {topic: 0 for topic in TOPICS}


def csv_path(topic: str) -> Path:
    return DATOS / f"{topic}.csv"


def append_row(topic: str, row: dict) -> None:
    DATOS.mkdir(parents=True, exist_ok=True)
    path = csv_path(topic)
    fieldnames = CAMPOS[topic]
    limpio = {campo: row.get(campo, "") for campo in fieldnames}
    write_header = (not path.exists()) or path.stat().st_size == 0
    with open(path, "a", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(limpio)
        handle.flush()
        os.fsync(handle.fileno())
    CONTADORES[topic] += 1


def crear_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id="obelie-consumer",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                key_deserializer=lambda b: b.decode("utf-8") if b else None,
            )
            print(
                f"[consumer] Conectado a {BOOTSTRAP_SERVERS}. Topics: {', '.join(TOPICS)}",
                flush=True,
            )
            return consumer
        except errores_sin_broker():
            print("[consumer] Kafka no disponible, reintentando en 3s...", flush=True)
            time.sleep(3)


def main() -> None:
    print("[consumer] Iniciando consumer unico de Obelie.", flush=True)
    consumer = crear_consumer()
    try:
        for mensaje in consumer:
            topic = mensaje.topic
            valor = mensaje.value
            if not isinstance(valor, dict):
                print(f"[consumer] Mensaje ignorado en {topic}: {valor!r}", flush=True)
                continue
            try:
                append_row(topic, valor)
            except OSError as exc:
                print(f"[consumer] Error escribiendo {topic}.csv: {exc}", flush=True)
                time.sleep(0.2)
                append_row(topic, valor)
            total = CONTADORES[topic]
            if total % 10 == 0:
                print(
                    f"[consumer] {topic}.csv += 10 (total sesion={total}) "
                    f"particion={mensaje.partition} offset={mensaje.offset}",
                    flush=True,
                )
    except KeyboardInterrupt:
        print("[consumer] Detenido por el usuario.", flush=True)
    finally:
        consumer.close()
        print(f"[consumer] Totales de sesion: {CONTADORES}", flush=True)


if __name__ == "__main__":
    main()
