from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "capstone_processing",
    default_args=default_args,
    schedule_interval="5 * * * *",
    catchup=False,
) as dag:

    start_dag = EmptyOperator(task_id="start_dag")

    tag_tasks = []
    tags = ['python-polars', 'airflow', 'apache-spark', 'dbt', 'docker', 'pyspark',  'sql']

    for tag in tags:
        tag_tasks.append(
            DockerOperator(
                task_id=f"docker_command_{tag}",
                image="capstone:mattias",
                container_name=f"capstone_mattias_{tag}",
                api_version="auto",
                auto_remove=True,
                command="/bin/sleep 30",
                docker_url="unix://var/run/docker.sock",
                network_mode="bridge",
                )
                )


    start_dag >> tag_tasks
