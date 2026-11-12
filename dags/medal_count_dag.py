import random
import time
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

CONN_ID = "goit_mysql_db"
TABLE = "medal_counts"
MEDALS = ["Bronze", "Silver", "Gold"]


def pick_medal():
    medal = random.choice(MEDALS)
    print(f"Picked medal: {medal}")
    return medal


def choose_branch(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def generate_delay(**context):
    delay = int(context["params"]["delay_seconds"])
    print(f"Sleeping {delay} seconds")
    time.sleep(delay)


with DAG(
    dag_id="medal_count_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={"delay_seconds": Param(5, type="integer", minimum=0)},
    tags=["goit"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at DATETIME
        );
        """,
    )

    pick = PythonOperator(task_id="pick_medal", python_callable=pick_medal)

    branch = BranchPythonOperator(task_id="pick_medal_task", python_callable=choose_branch)

    calc_tasks = [
        SQLExecuteQueryOperator(
            task_id=f"calc_{m}",
            conn_id=CONN_ID,
            sql=f"""
            INSERT INTO {TABLE} (medal_type, count, created_at)
            SELECT '{m}', COUNT(*), NOW()
            FROM athlete_event_results
            WHERE medal = '{m}';
            """,
        )
        for m in MEDALS
    ]

    delay = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=CONN_ID,
        sql=f"SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= 30 FROM {TABLE};",
        mode="poke",
        poke_interval=5,
        timeout=10,
    )

    create_table >> pick >> branch >> calc_tasks >> delay >> check_for_correctness
