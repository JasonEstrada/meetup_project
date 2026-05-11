# Documentación del Proyecto Meetup ETL

## 1. Visión General

Este proyecto es una solución ETL construida con Apache Airflow y Snowflake, diseñada para cargar datos que simulan ser batchs nuevos desde archivos CSV, prepararlos en un área de staging, hacer merge/upsert hacia tablas finales y, finalmente, exportar resultados a un stage de S3.

El repositorio está compuesto principalmente por:
- `docker-compose.yaml`: infraestructura de contenedores para Airflow y PostgreSQL.
- `requirements.txt`: dependencias Python necesarias.
- `airflow/dags/`: definición de los DAGs de Airflow.
- `airflow/data/`: archivos CSV fuente del proyecto.
- `airflow/logs/`: logs de ejecución de Airflow.
- `airflow/dags/callbacks.py`: callbacks para notificaciones en Slack.

## 2. Arquitectura y componentes

### 2.1. Infraestructura Docker

El proyecto se levanta con Docker Compose y contiene dos servicios:
- `db`: base de datos PostgreSQL usada por Airflow.
- `af`: contenedor de Apache Airflow 3.0 que monta los DAGs, datos y plugins.

El servicio `af` ejecuta al iniciar:
- instalación de dependencias desde `requirements.txt`
- migración de la base de datos de Airflow
- comando `airflow standalone` para levantar el scheduler/webserver/local executor.

### 2.2. Conexiones externas

Airflow se integra con Snowflake mediante la conexión `snowflake_default` y con Slack mediante `slack_default`.

### 2.3. Flujo general de datos

1. Se cargan archivos CSV desde `airflow/data/` a un stage interno de Snowflake.
2. Se identifica el archivo más reciente para cada tabla.
3. Se crean tablas de staging (`STG_...`) en Snowflake y se copian datos desde el stage.
4. Se aplican operaciones `MERGE` para sincronizar los datos staging con las tablas finales.
   - Nota: las tablas finales deben existir previamente en Snowflake.
   - En este proyecto no se crean las tablas finales desde Airflow porque la infraestructura de base de datos no se construye con Airflow y no es una buena práctica hacerlo desde la orquestación.
5. Se exportan las tablas finales a un stage de S3.
6. Se envían notificaciones de éxito o error por Slack para cada DAG.

## 3. DAGs principales

### 3.1. `load_csv_to_stage`

- `dag_id`: `load_csv_to_stage`
- `schedule`: cada 15 minutos (`*/15 * * * *`)
- Objetivo: subir archivos CSV que simulan ser batchs nuevos desde `airflow/data/` a un stage interno de Snowflake llamado `meetup_stage`.
- Lógica:
  - Conecta a Snowflake usando las credenciales de Airflow.
  - Crea el stage si no existe.
  - Sube cada CSV al stage con un nombre versionado por timestamp.
  - Dispara el DAG `stage_to_stg_tables` y espera su finalización.

### 3.2. `stage_to_stg_tables`

- `dag_id`: `stage_to_stg_tables`
- `schedule`: manual / disparado por `load_csv_to_stage`
- Objetivo: cargar los archivos más recientes desde el stage de Snowflake a tablas de staging (`STG_*`).
- Lógica:
  - Crea un formato de archivo CSV en Snowflake.
  - Para cada entidad del proyecto (`categories`, `cities`, `events`, `groups`, `groups_topics`, `members`, `members_topics`, `topics`, `venues`):
    - localiza el archivo más reciente en el stage.
    - crea o reemplaza la tabla `STG_<TABLE>` usando inferencia de esquema.
    - copia los datos del archivo al `STG_<TABLE>`.
  - Al terminar, dispara el DAG `upsert_tables`.

### 3.3. `upsert_tables`

- `dag_id`: `upsert_tables`
- `schedule`: manual / disparado por `stage_to_stg_tables`
- Objetivo: aplicar MERGE/UPSERT desde las tablas staging hacia las tablas finales en Snowflake.
- Lógica:
  - Define la configuración de cada tabla final, incluyendo clave primaria y columnas mapeadas.
  - Para cada tabla en `TABLES_CONFIG` ejecuta una función Python que:
    - genera SQL `MERGE INTO` usando la tabla final y su tabla staging.
    - actualiza registros existentes y crea nuevos si no existen.
  - Al concluir todas las tablas, dispara el DAG `export_to_s3`.

### 3.4. `export_to_s3`

- `dag_id`: `export_to_s3`
- `schedule`: manual / disparado por `upsert_tables`
- Objetivo: exportar las tablas finales de Snowflake a un stage de S3.
- Lógica:
  - Para cada tabla final listada en `TABLES` ejecuta un `COPY INTO` hacia el stage S3.
  - Configura el formato CSV con encabezado y sin compresión.

## 4. Monitoreo y notificaciones

### `airflow/dags/callbacks.py`

- Define `on_success` y `on_failure` para enviar mensajes a Slack.
- Usa `SlackHook` con la conexión `slack_default` y canal `alertas-airflow`.
- El mensaje informa:
  - DAG
  - Task
  - Estado
  - Run ID

## 5. Datos del proyecto

Los archivos CSV fuente se encuentran en `airflow/data/` y contienen:
- `categories.csv`
- `cities.csv`
- `events.csv`
- `groups_topics.csv`
- `groups.csv`
- `members_topics.csv`
- `members.csv`
- `topics.csv`
- `venues.csv`

Los datos fueron obtenidos de https://www.kaggle.com/datasets/megelon/meetup.

En este proyecto se intenta simular un flujo de ingestión por lotes (batch), donde cada archivo actúa como un nuevo batch de datos.
Por esa razón los archivos se versionan con timestamp en el stage y se aplica un `MERGE` para actualizar los datos existentes y mantener la integración incremental.

Estas fuentes son utilizadas para poblar:
- Dimensiones (`DIM_*`)
- Tablas de hechos (`FACT_EVENTS`)
- Tablas puente (`BRIDGE_*`)

## 6. De manera general

- Se implementó un pipeline ETL modular con Airflow y Snowflake.
- El flujo está orquestado en 4 DAGs encadenados: carga, staging, upsert y exportación.
- Se maneja control de versiones de archivos con timestamps en el stage.
- Se usa el patrón `STG -> final` y operaciones `MERGE` para sincronización incremental.
- Se incluye observabilidad con callbacks de Slack para éxito/fallo.
- La solución está containerizada usando Docker Compose.

## 7. Cómo correr el proyecto

1. Levantar los contenedores:
   ```bash
   docker-compose up --build
   ```
2. Acceder a la UI de Airflow en `http://localhost:8080`.
3. Verificar la conexión `snowflake_default` y `slack_default` en Airflow.
4. Ejecutar el DAG `load_csv_to_stage` o esperar la programación cada 15 minutos.

---

Esta documentación está pensada para explicar rápidamente la estructura del proyecto y el propósito de cada componente.
