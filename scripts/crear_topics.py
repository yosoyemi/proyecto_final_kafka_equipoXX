"""Crea los 4 topics de Obelie: 3 particiones, replication factor 1."""

from __future__ import annotations

import sys
import time
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from catalogo import BOOTSTRAP_SERVERS, TOPICS, errores_sin_broker


def esperar_kafka(timeout: int = 90) -> KafkaAdminClient:
    inicio = time.time()
    ultimo_error = None
    while time.time() - inicio < timeout:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id="obelie-crear-topics",
                request_timeout_ms=8000,
            )
            print(f"[topics] Kafka disponible en {BOOTSTRAP_SERVERS}")
            return admin
        except errores_sin_broker() as exc:
            ultimo_error = exc
            print("[topics] Esperando a Kafka...")
            time.sleep(3)
    raise SystemExit(f"No se pudo conectar a Kafka: {ultimo_error}")


def main() -> None:
    admin = esperar_kafka()
    existentes = set(admin.list_topics())
    nombres_nuevos = [nombre for nombre in TOPICS if nombre not in existentes]
    nuevos = [NewTopic(name=nombre, num_partitions=3, replication_factor=1) for nombre in nombres_nuevos]
    try:
        if nuevos:
            admin.create_topics(new_topics=nuevos, validate_only=False)
            print(f"[topics] Creados: {', '.join(nombres_nuevos)}")
        else:
            print("[topics] Todos los topics ya existian.")
    except TopicAlreadyExistsError:
        print("[topics] Algunos topics ya existian.")
    except Exception as exc:
        mensaje = str(exc)
        if "already exists" in mensaje.lower() or "TopicExistsException" in mensaje:
            print("[topics] Topics ya existentes (se conservan).")
        else:
            print(f"[topics] Aviso al crear: {exc}")

    # La propagacion de metadatos puede tardar un instante despues de crearlos.
    limite = time.time() + 15
    while time.time() < limite:
        existentes = set(admin.list_topics())
        if all(topic in existentes for topic in TOPICS):
            break
        time.sleep(1)
    faltantes = [t for t in TOPICS if t not in existentes]
    if faltantes:
        admin.close()
        raise SystemExit(f"Faltan topics: {faltantes}")

    descripcion = admin.describe_topics(list(TOPICS))
    for topic in descripcion:
        nombre = topic["topic"] if isinstance(topic, dict) else topic
        if isinstance(topic, dict):
            partes = topic.get("partitions", [])
            print(f"[topics] {nombre}: {len(partes)} particiones")
        else:
            print(f"[topics] {nombre}")
    admin.close()
    print("[topics] Listo.")


if __name__ == "__main__":
    main()
