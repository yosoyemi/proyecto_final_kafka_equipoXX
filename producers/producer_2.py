"""Producer 2: publica eventos de clientes en Kafka."""

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
    CIUDADES,
    NOMBRES,
    SUCURSALES,
    TIPOS_CLIENTE,
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
            print(f"[producer_2] Conectado a Kafka en {BOOTSTRAP_SERVERS}", flush=True)
            return producer
        except errores_sin_broker():
            print("[producer_2] Kafka no disponible, reintentando en 3s...", flush=True)
            time.sleep(3)


def generar_cliente() -> dict:
    nombre = random.choice(NOMBRES)
    slug = (
        nombre.lower()
        .replace(" ", ".")
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )
    return {
        "id_cliente": f"C-{uuid.uuid4().hex[:10].upper()}",
        "fecha_hora": ahora(),
        "nombre": nombre,
        "email": f"{slug}.{random.randint(10, 99)}@obelie.mx",
        "telefono": f"33{random.randint(1000, 9999)}{random.randint(1000, 9999)}",
        "ciudad": random.choice(CIUDADES),
        "tipo_cliente": random.choice(TIPOS_CLIENTE),
        "sucursal_preferida": random.choice(SUCURSALES),
    }


def main() -> None:
    print("[producer_2] Iniciando. Topic: clientes", flush=True)
    producer = crear_producer()
    enviados = 0
    try:
        while True:
            cliente = generar_cliente()
            producer.send("clientes", key=cliente["id_cliente"], value=cliente)
            enviados += 1
            if enviados % 5 == 0:
                producer.flush()
                print(
                    f"[producer_2] {enviados} clientes | ultimo={cliente['id_cliente']} "
                    f"{cliente['nombre']} ({cliente['tipo_cliente']})",
                    flush=True,
                )
            time.sleep(random.uniform(0.4, 0.9))
    except KeyboardInterrupt:
        print("[producer_2] Detenido por el usuario.", flush=True)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
