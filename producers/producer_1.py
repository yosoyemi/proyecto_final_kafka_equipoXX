"""Producer 1: publica eventos de ventas y pagos en Kafka."""

from __future__ import annotations

import json
import random
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from catalogo import (
    BOOTSTRAP_SERVERS,
    METODOS_PAGO,
    PRODUCTOS,
    SUCURSALES,
    VENDEDORES,
    errores_sin_broker,
)


def ahora() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def crear_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                linger_ms=20,
            )
            print(f"[producer_1] Conectado a Kafka en {BOOTSTRAP_SERVERS}", flush=True)
            return producer
        except errores_sin_broker():
            print("[producer_1] Kafka no disponible, reintentando en 3s...", flush=True)
            time.sleep(3)


def generar_venta() -> dict:
    producto = random.choice(PRODUCTOS)
    cantidad = random.randint(1, 6)
    precio = producto["precio"]
    total = round(cantidad * precio, 2)
    return {
        "id_venta": f"V-{uuid.uuid4().hex[:10].upper()}",
        "fecha_hora": ahora(),
        "producto": producto["nombre"],
        "cantidad": cantidad,
        "precio_unitario": precio,
        "total": total,
        "sucursal": random.choice(SUCURSALES),
        "vendedor": random.choice(VENDEDORES),
    }


def generar_pago(venta: dict) -> dict:
    metodo = random.choice(METODOS_PAGO)
    estados = ["aprobado", "aprobado", "aprobado", "pendiente"]
    return {
        "id_pago": f"P-{uuid.uuid4().hex[:10].upper()}",
        "id_venta": venta["id_venta"],
        "fecha_hora": ahora(),
        "metodo_pago": metodo,
        "monto": venta["total"],
        "estado": random.choice(estados),
        "referencia": f"REF-{uuid.uuid4().hex[:8].upper()}",
    }


def main() -> None:
    print("[producer_1] Iniciando. Topics: ventas, pagos", flush=True)
    producer = crear_producer()
    enviados = 0
    try:
        while True:
            venta = generar_venta()
            pago = generar_pago(venta)
            producer.send("ventas", key=venta["id_venta"], value=venta)
            producer.send("pagos", key=pago["id_venta"], value=pago)
            enviados += 1
            if enviados % 5 == 0:
                producer.flush()
                print(
                    f"[producer_1] {enviados} ventas/pagos | ultima venta={venta['id_venta']} "
                    f"{venta['producto']} ${venta['total']} | pago={pago['metodo_pago']}",
                    flush=True,
                )
            time.sleep(random.uniform(0.35, 0.8))
    except KeyboardInterrupt:
        print("[producer_1] Detenido por el usuario.", flush=True)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
