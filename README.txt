OBELIE PASTELERIA
Sistema de streaming de ventas con Apache Kafka + Python + Pandas + Streamlit
Proyecto final - proyecto_final_kafka_equipoXX
================================================================================

1. QUE HACE ESTE PROYECTO
-------------------------
Simula en tiempo real la operacion de Obelie Pasteleria:

  3 Producers  -->  4 Topics Kafka  -->  1 Consumer  -->  4 CSV (respaldo si falla Supabase)
  POS real Obelie (Supabase) -------------------------------------> Streamlit (fuente principal)

Los producers generan eventos JSON automaticos (ventas, pagos, clientes e inventario).
Kafka los transporta en 4 topics. Un solo consumer los escucha al mismo tiempo y
va escribiendo (append) cuatro archivos CSV. El dashboard de Streamlit muestra
solo el POS real en Supabase. Los CSV de Kafka se usan unicamente si no hay
conexion con Supabase.


2. ARQUITECTURA
---------------
Producers:
  - producers/producer_1.py  -> topics "ventas" y "pagos" (el pago usa el mismo id_venta)
  - producers/producer_2.py  -> topic "clientes"
  - producers/producer_3.py  -> topic "inventario"

Topics Kafka (localhost:9092):
  - ventas
  - clientes
  - pagos
  - inventario
  Cada uno: 3 particiones, replication factor 1.

Consumer:
  - consumer/consumer.py
  Escucha los 4 topics a la vez y genera/actualiza:
    datos/ventas.csv
    datos/clientes.csv
    datos/pagos.csv
    datos/inventario.csv
  Los CSV tienen encabezados. Las filas nuevas se agregan; no se borran las anteriores.
  El consumer es quien crea los archivos.

Dashboard:
  - dashboard/dashboard.py
  Titulo: Obelie / Ventas en tiempo real
  Fuente: POS real en Supabase (proyecto "obelie")
  Respaldo: CSV de Kafka solo si Supabase no responde
  Tema claro neutro, texto oscuro y graficas de un solo color.
  Metricas: Total vendido, Numero de ventas, Clientes, Stock total
  Graficas: Ventas en el tiempo, Ventas por producto, Metodos de pago, Stock por producto
  Tabla de ultimas 10 ventas
  Integracion ventas + pagos por id_venta
  Lectura segura si un CSV todavia no existe o se esta escribiendo.
  El cliente de solo lectura esta en obelie_supabase.py (no escribe al POS).

Kafka corre en localhost:9092 mediante Docker (ver docker-compose.yml) o mediante
el respaldo local para Windows de scripts/iniciar_kafka_local.ps1.


3. REQUISITOS
-------------
- Windows 10/11
- Docker Desktop (recomendado). Si no esta disponible, el inicio automatico
  descarga y ejecuta Kafka local usando Java 17+.
- Python 3.11 o superior (se probo con Python 3.13)
- Puerto 9092 libre (Kafka) y puerto 8501 libre (Streamlit)


4. INSTALACION (paso a paso)
----------------------------
A) Elegir una de estas opciones para Kafka:
   - Docker Desktop en ejecucion (recomendado), o
   - Java 17 o superior. El inicio automatico prepara Kafka local.

B) Abrir PowerShell en la carpeta del proyecto:

     cd ruta\proyecto_final_kafka_equipoXX

C) Crear el entorno virtual e instalar dependencias:

     py -3 -m venv .venv
     .\.venv\Scripts\python.exe -m pip install --upgrade pip
     .\.venv\Scripts\python.exe -m pip install -r requirements.txt


5. COMO EJECUTAR TODO
---------------------
Opcion recomendada: doble clic en INICIAR.bat desde el Explorador de Windows.
Esta en la carpeta proyecto_final_kafka_equipoXX, un nivel arriba de .venv.
Espera el mensaje "Sistema iniciado"; se abrira el navegador automaticamente.

Tambien puedes ejecutar desde la carpeta del proyecto:

     powershell -ExecutionPolicy Bypass -File .\scripts\iniciar_sistema.ps1

Ese script:
  1. Levanta Kafka con Docker; si Docker no esta disponible, usa Kafka local
     en Windows (la primera ejecucion descarga aproximadamente 116 MB)
  2. Crea los 4 topics (3 particiones, RF=1)
  3. Arranca el consumer
  4. Arranca los 3 producers
  5. Arranca Streamlit en http://localhost:8501
  6. Comprueba que el dashboard responde y que los cuatro CSV reciben datos nuevos
  7. Abre el navegador. Si algo falla, informa del error en vez de anunciar exito

Puedes volver a ejecutar INICIAR.bat: reutiliza los procesos que ya estan activos.
Para iniciar sin abrir otra pestana, agrega -SinNavegador al comando PowerShell.
Si tienes el panel anterior abierto, actualiza la pagina con Ctrl+F5.

Opcion manual (para evidencias / captura de terminales):

  1. Kafka:
       docker compose up -d

     Sin Docker:
       powershell -ExecutionPolicy Bypass -File .\scripts\iniciar_kafka_local.ps1

  2. Esperar a que Kafka este sano y crear topics:
       .\.venv\Scripts\python.exe .\scripts\crear_topics.py

     (docker compose tambien crea los topics con el servicio kafka-init)

  3. Consumer (una terminal):
       .\.venv\Scripts\python.exe .\consumer\consumer.py

  4. Producers (tres terminales):
       .\.venv\Scripts\python.exe .\producers\producer_1.py
       .\.venv\Scripts\python.exe .\producers\producer_2.py
       .\.venv\Scripts\python.exe .\producers\producer_3.py

  5. Dashboard:
       .\.venv\Scripts\python.exe -m streamlit run .\dashboard\dashboard.py --server.port 8501

Abrir el navegador en:

     http://localhost:8501


6. VERIFICAR QUE ESTA LISTO
---------------------------
Con el sistema corriendo:

     .\.venv\Scripts\python.exe .\scripts\verificar_sistema.py

Si absolutamente todo pasa, el script termina mostrando:

     PROYECTO LISTO PARA ENTREGAR

Comprueba:
  - Kafka en localhost:9092
  - 4 topics
  - 3 particiones por topic
  - 3 producers activos
  - 1 consumer activo
  - 4 CSV con encabezados y minimo 20 registros cada uno
  - Dashboard Streamlit
  - README.txt


7. DETENER
----------
Doble clic en DETENER.bat para cerrar los procesos del proyecto y Kafka local.
Los CSV y el almacenamiento de Kafka se conservan.

Procesos Python (producers, consumer, dashboard):

     powershell -ExecutionPolicy Bypass -File .\scripts\detener_sistema.ps1

Procesos Python + Kafka:

     powershell -ExecutionPolicy Bypass -File .\scripts\detener_sistema.ps1 -Kafka

O manualmente:

     docker compose down


8. ESTRUCTURA DE CARPETAS
-------------------------
proyecto_final_kafka_equipoXX/
  docker-compose.yml      Kafka KRaft (sin Zookeeper) en el puerto 9092
  requirements.txt        kafka-python-ng, pandas, streamlit, plotly, supabase, python-dotenv
  obelie_supabase.py      Cliente de lectura del POS real (Supabase)
  .env.example            URL y clave publicable del proyecto obelie
  README.txt              Este archivo
  catalogo.py             Productos, sucursales y configuracion compartida
  producers/              3 producers
  consumer/               1 consumer
  dashboard/              Streamlit
  datos/                  CSV generados por el consumer (no borrar a mano)
  evidencias/             Carpeta para capturas reales de pantalla
  scripts/                crear_topics, verificar_sistema, iniciar, detener
  logs/                   Salida de los procesos si se usa iniciar_sistema.ps1


9. CAMPOS DE LOS EVENTOS (JSON, minimo 6 + fecha/hora)
------------------------------------------------------
ventas:      id_venta, fecha_hora, producto, cantidad, precio_unitario, total, sucursal, vendedor
pagos:       id_pago, id_venta, fecha_hora, metodo_pago, monto, estado, referencia
clientes:    id_cliente, fecha_hora, nombre, email, telefono, ciudad, tipo_cliente, sucursal_preferida
inventario:  id_evento, fecha_hora, producto, stock, sucursal, categoria, unidad, costo_unitario


10. PRUEBAS SUGERIDAS (Y EVIDENCIAS)
------------------------------------
La carpeta evidencias/ esta vacia a proposito. Toma capturas REALES de:

  1. Docker Desktop / docker compose ps con kafka-obelie
  2. docker compose logs kafka-init  (creacion de topics)
  3. Las 3 terminales de producers generando JSON
  4. La terminal del consumer escribiendo CSV
  5. Los 4 archivos en datos/ con mas de 20 filas
  6. El dashboard Streamlit (metricas y 4 graficas)
  7. Detener un producer (Ctrl+C) y ver que los otros siguen
     y que sus CSV siguen creciendo
  8. Reiniciar ese producer
  9. Salida de scripts/verificar_sistema.py con
     "PROYECTO LISTO PARA ENTREGAR"

Comandos utiles durante la prueba:

  docker compose ps
  docker exec kafka-obelie /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

  Contar registros (sin encabezado):

     .\.venv\Scripts\python.exe -c "import pathlib; p=pathlib.Path('datos');
print({f.name: max(0,len(f.read_text(encoding='utf-8-sig').splitlines())-1) for f in p.glob('*.csv')})"


11. NOTAS
---------
- El consumer crea los CSV. No hace falta crearlos a mano.
- Si reinicias los producers, los CSV conservan el historial (modo append).
- Si Kafka tarda en arrancar, producers y consumer reintentan solos cada 3 segundos.
- El dashboard no se cae si un CSV aun no existe: muestra un aviso y sigue refrescando.
- El panel muestra solo ventas, pagos, clientes e inventario reales de
  https://rydymvahvuogjuiuyuzq.supabase.co. Si no hay conexion, usa los CSV de Kafka.
- Copia .env.example a .env si cambias de proyecto. La clave publicable es de
  solo lectura para el dashboard; los producers de Kafka no escriben en Supabase.
