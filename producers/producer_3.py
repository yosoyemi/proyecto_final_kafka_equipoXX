"""Producer 3: publica snapshots de inventario en Kafka."""

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

from catalogo import BOOTSTRAP_SERVERS, PRODUCTOS, SUCURSALES, errores_sin_broker


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
            print(f"[producer_3] Conectado a Kafka en {BOOTSTRAP_SERVERS}", flush=True)
            return producer
        except errores_sin_broker():
            print("[producer_3] Kafka no disponible, reintentando en 3s...", flush=True)
            time.sleep(3)


def stock_inicial() -> dict[tuple[str, str], int]:
    stock: dict[tuple[str, str], int] = {}
    for sucursal in SUCURSALES:
        for producto in PRODUCTOS:
            stock[(producto["nombre"], sucursal)] = random.randint(20, 80)
    return stock


def generar_inventario(stock: dict[tuple[str, str], int]) -> dict:
    producto = random.choice(PRODUCTOS)
    sucursal = random.choice(SUCURSALES)
    clave = (producto["nombre"], sucursal)
    delta = random.randint(-8, 12)
    stock[clave] = max(0, stock[clave] + delta)
    return {
        "id_evento": f"I-{uuid.uuid4().hex[:10].upper()}",
        "fecha_hora": ahora(),
        "producto": producto["nombre"],
        "stock": stock[clave],
        "sucursal": sucursal,
        "categoria": producto["categoria"],
        "unidad": producto["unidad"],
        "costo_unitario": producto["costo"],
    }


def main() -> None:
    print("[producer_3] Iniciando. Topic: inventario", flush=True)
    producer = crear_producer()
    stock = stock_inicial()
    enviados = 0
    try:
        while True:
            evento = generar_inventario(stock)
            producer.send("inventario", key=evento["producto"], value=evento)
            enviados += 1
            if enviados % 5 == 0:
                producer.flush()
                print(
                    f"[producer_3] {enviados} inventario | {evento['producto']} "
                    f"stock={evento['stock']} sucursal={evento['sucursal']}",
                    flush=True,
                )
            time.sleep(random.uniform(0.35, 0.8))
    except KeyboardInterrupt:
        print("[producer_3] Detenido por el usuario.", flush=True)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
