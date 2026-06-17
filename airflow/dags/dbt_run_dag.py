from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["admin@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_run_dag",
    default_args=default_args,
    description="dbt 批量建模: staging -> marts -> metrics",
    schedule_interval="30 0 * * *",
    start_date=datetime(2026, 6, 18),
    catchup=False,
    tags=["dbt", "batch", "daily"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps --profiles-dir .",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt && dbt run --models staging --profiles-dir .",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt && dbt run --models marts --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
    )

    dbt_deps >> dbt_run_staging >> dbt_run_marts >> dbt_test
