
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'example_dag_5',
    default_args=default_args,
    description='A simple example DAG with 100 tasks',
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

tasks = []
for i in range(1, 101):
    task = DummyOperator(
        task_id=f'task_{i}',
        dag=dag,
    )
    tasks.append(task)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> tasks[0]
for i in range(99):
    tasks[i] >> tasks[i + 1]
tasks[-1] >> end
