from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1), # Data ajustada para o ciclo atual
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Puxa a rede definida no seu arquivo .env (datalake_xxxxxx_airflow)
docker_network = os.getenv("DOCKER_NETWORK")

with DAG(
    dag_id="turismo_serra_2",
    schedule_interval=None, # Rodar manualmente ou via trigger
    catchup=False,
    default_args=default_args,
    tags=["turismo", "serra", "iss"],
) as dag:

    # 1. Fase Bronze: Extração dos CSVs para o MinIO
    coleta = DockerOperator(
        task_id="coleta_turismo_serra",
        image="turismo_serra_2-coleta:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=docker_network,
        mount_tmp_dir=False,
    )

    # 2. Fase Silver: Limpeza e Tipagem (Bairros/Cidades)
    pre_processamento = DockerOperator(
        task_id="pre_processamento_turismo_serra",
        image="turismo_serra_2-pre_processamento:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=docker_network,
        mount_tmp_dir=False,
    )

    # 3. Fase Gold: Indicadores Econômicos (Sazonalidade e Índices)
    # Note que aqui a imagem termina em '-processamento' conforme você pediu
    processamento = DockerOperator(
        task_id="processamento_turismo_serra",
        image="turismo_serra_2-processamento:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=docker_network,
        mount_tmp_dir=False,
    )

    # Ordem de execução: Primeiro coleta, depois limpa, por fim gera indicadores.
    coleta >> pre_processamento >> processamento
