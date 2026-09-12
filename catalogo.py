"""Catalogo compartido de Obelie Pasteleria y configuracion de Kafka."""

BOOTSTRAP_SERVERS = "127.0.0.1:9092"

TOPICS = ("ventas", "clientes", "pagos", "inventario")

PRODUCTOS = [
    {
        "nombre": "Pastel de chocolate",
        "precio": 380.0,
        "categoria": "Pasteles",
        "unidad": "pza",
        "costo": 150.0,
    },
    {
        "nombre": "Cheesecake de frutos rojos",
        "precio": 420.0,
        "categoria": "Pasteles",
        "unidad": "pza",
        "costo": 170.0,
    },
    {
        "nombre": "Croissant de almendra",
        "precio": 48.0,
        "categoria": "Viennoiserie",
        "unidad": "pza",
        "costo": 18.0,
    },
    {
        "nombre": "Rol de canela",
        "precio": 42.0,
        "categoria": "Viennoiserie",
        "unidad": "pza",
        "costo": 15.0,
    },
    {
        "nombre": "Macaron mixto",
        "precio": 28.0,
        "categoria": "Petit fours",
        "unidad": "pza",
        "costo": 10.0,
    },
    {
        "nombre": "Eclair de cafe",
        "precio": 45.0,
        "categoria": "Petit fours",
        "unidad": "pza",
        "costo": 16.0,
    },
    {
        "nombre": "Brownie nogada",
        "precio": 55.0,
        "categoria": "Reposteria",
        "unidad": "pza",
        "costo": 20.0,
    },
    {
        "nombre": "Galleta de mantequilla",
        "precio": 18.0,
        "categoria": "Reposteria",
        "unidad": "pza",
        "costo": 6.0,
    },
    {
        "nombre": "Tarta de frutas",
        "precio": 390.0,
        "categoria": "Pasteles",
        "unidad": "pza",
        "costo": 155.0,
    },
    {
        "nombre": "Baguette tradicional",
        "precio": 32.0,
        "categoria": "Panaderia",
        "unidad": "pza",
        "costo": 9.0,
    },
]

SUCURSALES = ["Centro", "Norte", "Sur", "Plaza Obelie"]

VENDEDORES = [
    "Lucia Mendoza",
    "Carlos Rivas",
    "Ana Sofia Cruz",
    "Diego Palacios",
    "Mariana Solis",
]

METODOS_PAGO = ["efectivo", "tarjeta", "transferencia", "vale"]

NOMBRES = [
    "Elena Vargas",
    "Jorge Castillo",
    "Paola Nunez",
    "Ricardo Flores",
    "Camila Herrera",
    "Andres Molina",
    "Sofia Ramirez",
    "Fernando Diaz",
    "Valeria Ortiz",
    "Hugo Salazar",
    "Daniela Pineda",
    "Mateo Aguilar",
]

CIUDADES = ["Guadalajara", "Zapopan", "Tlaquepaque", "Tonalá", "Tlajomulco"]

TIPOS_CLIENTE = ["frecuente", "nuevo", "mayoreo", "evento"]


def errores_sin_broker() -> tuple[type[BaseException], ...]:
    """Errores de broker ausente en kafka-python-ng 2.x y kafka-python 3.x.

    kafka-python 3.0 elimino NoBrokersAvailable. Ambos paquetes publican el
    modulo `kafka` y no pueden coexistir; este helper acepta cualquiera.
    """
    from kafka.errors import KafkaConnectionError, KafkaTimeoutError

    errores: list[type[BaseException]] = [KafkaConnectionError, KafkaTimeoutError]
    try:
        from kafka.errors import NoBrokersAvailable
    except ImportError:
        pass
    else:
        errores.insert(0, NoBrokersAvailable)
    return tuple(errores)
