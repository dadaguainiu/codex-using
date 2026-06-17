from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import json
import logging

log = logging.getLogger("data_quality")

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["admin@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def check_kafka_lag():
    """检查 Kafka 消费者延迟"""
    import subprocess
    result = subprocess.run(
        ["kafka-consumer-groups.sh", "--bootstrap-server", "kafka:9092",
         "--group", "flink-cdc-group", "--describe"],
        capture_output=True, text=True, timeout=30
    )
    log.info("Kafka lag:\n%s", result.stdout)
    return result.stdout

def check_paimon_snapshots():
    """检查 Paimon 快照数量"""
    import os, glob
    snapshot_dirs = glob.glob("/data/paimon/warehouse/**/snapshot/*", recursive=True)
    log.info("Paimon snapshots count: %d", len(snapshot_dirs))
    return {"snapshot_count": len(snapshot_dirs)}

def report_quality():
    """汇总质量检查结果"""
    log.info("Data quality check completed at %s", datetime.now().isoformat())

with DAG(
    dag_id="data_quality_dag",
    default_args=default_args,
    description="数据质量检查 & 性能监控",
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 6, 18),
    catchup=False,
    tags=["governance", "quality"],
) as dag:

    kafka_lag_check = PythonOperator(
        task_id="check_kafka_lag",
        python_callable=check_kafka_lag,
    )

    paimon_check = PythonOperator(
        task_id="check_paimon_snapshots",
        python_callable=check_paimon_snapshots,
    )

    start_check = BashOperator(
        task_id="start_check",
        bash_command="echo 'Data quality check started at $(date)'",
    )

    report = PythonOperator(
        task_id="report_quality",
        python_callable=report_quality,
    )

    start_check >> [kafka_lag_check, paimon_check] >> report
