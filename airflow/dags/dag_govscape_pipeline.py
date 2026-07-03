# airflow/dags/dag_govscape_pipeline.py
"""
DAG: govScape Legislative Data Pipeline
Arquitectura: DockerOperator pattern — Airflow es el Manager,
              govscape-pipeline:prod es el Chef especialista.

PREREQUISITO: Antes de levantar docker-compose, correr:
  make build   # Construye govscape-pipeline:prod
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# =========================================================
# CONFIGURACIÓN BASE
# =========================================================

# Ruta ABSOLUTA de tu carpeta data en el host (tu Mac).
# DockerOperator monta esto directamente — Airflow no toca los archivos.
# Cámbiala por tu ruta real: echo $PWD en la carpeta del proyecto
# HOST_DATA_PATH = "/Users/erik/project_govScape/data"

# Ruta ABSOLUTA de tu carpeta data en el host (tu Mac).
# DockerOperator monta esto directamente — Airflow no toca los archivos.
# Cámbiala por tu ruta real: echo $PWD en la carpeta del proyecto
HOST_DATA_PATH = "/Users/erikmendozaruiz/Documents/data_engineer_project2026/project_govScape/data"

default_args = {
    "owner": "govScape-team",
    "depends_on_past": False,  # Cada run es independiente
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

# =========================================================
# CONFIGURACIÓN COMPARTIDA DEL CONTENEDOR
# Evita repetir los mismos parámetros en cada tarea
# =========================================================
DOCKER_COMMON = dict(
    image="govscape-pipeline:prod",  # La imagen que construyes con 'make build'
    auto_remove="success",  # Destruye el container al terminar → libera RAM
    docker_url="unix://var/run/docker.sock",  # El walkie-talkie al Docker Daemon
    network_mode="bridge",
    mount_tmp_dir=False,
    # Variables de entorno que tu config.py necesita leer
    environment={
        "CONGRESS_API_KEY": "{{ var.value.CONGRESS_API_KEY }}",  # Airflow Variable (seguro)
        "PYTHONPATH": "/app:/app/src",
    },
    mounts=[
        # Conecta ./data del host con /app/data dentro del container del pipeline
        # Así Bronze escribe archivos que Silver puede leer en el siguiente step
        Mount(
            source=HOST_DATA_PATH,
            target="/app/data",
            type="bind",
        )
    ],
)

# =========================================================
# DEFINICIÓN DEL DAG
# =========================================================
with DAG(
    dag_id="pipeline_govscape_principal",
    default_args=default_args,
    description="Pipeline legislativo Bronze → Silver → Gold via DockerOperator",
    schedule="22 4 * * *",  # Manual trigger por ahora ('0 6 * * *' para diario a las 6am)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["govscape", "pipeline", "medallion"],
) as dag:

    # ---------------------------------------------------------
    # TASK 1: Bronze — Ingesta de Legislators
    # Llama a src/ingest_comms_to_bronze.py::fetch_legislator_data()
    # ---------------------------------------------------------
    bronze_legislators = DockerOperator(
        task_id="bronze_ingest_legislators",
        command="python src/ingest_comms_to_bronze.py",
        **DOCKER_COMMON,
    )
    # ---------------------------------------------------------
    # TASK 3: Silver — Transform Legislators
    # Usa la fecha de ejecución real de Airflow (no datetime.now() hardcodeado)
    # {{ ds }} es una macro de Airflow que inyecta la fecha de la run: '2026-06-16'
    # ---------------------------------------------------------

    silver_legislators = DockerOperator(
        task_id="silver_transform_legislators",
        command="python src/transform_to_silver.py {{ ds }}",
        **DOCKER_COMMON,
    )

    # ---------------------------------------------------------
    # TASK 5: Gold — Analytics para Legislators
    # ---------------------------------------------------------
    gold_legislators = DockerOperator(
        task_id="gold_analyze_legislators",
        command="python src/analyze_legislators.py {{ ds }}",
        **DOCKER_COMMON,
    )

    # =========================================================
    # DEPENDENCY GRAPH — Las vías del tren (con paralelismo real)
    # Bronze corre en paralelo → cada Silver espera su Bronze → Gold espera ambos Silver
    #
    #  bronze_legislators ──► silver_legislators ──┐
    #                                              ├──► gold_legislators
    #  bronze_bills ────────► silver_bills ────────┘──► gold_bills
    # =========================================================
    bronze_legislators >> silver_legislators >> gold_legislators
    # bronze_bills >> silver_bills >> [gold_legislators, gold_bills]
