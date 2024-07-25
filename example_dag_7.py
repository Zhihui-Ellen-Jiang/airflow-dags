
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import requests

default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}}

dag_id = 'example_dag_7'

dag = DAG(
    dag_id,
    default_args=default_args,
    description='A DAG with meaningful tasks',
    schedule_interval=timedelta(minutes=10),
    max_active_runs=1,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

def fetch_url(task_number):
    url = f"https://jsonplaceholder.typicode.com/todos/{{}}".format(task_number)
    try:
        response = requests.get(url, timeout=10)
        print(f"Task {{}}: Fetched data: {{}}".format(task_number, response.json()))
    except requests.RequestException as e:
        print(f"Task {{}}: Request failed: {{}}".format(task_number, e))

def calculate_square(task_number):
    result = task_number ** 2
    print(f"Task {{}}: Square of {{}} is {{}}".format(task_number, task_number, result))

def log_message(task_number):
    print(f"Task {{}}: This is a log message.".format(task_number))

tasks = []
for i in range(1, 101):
    if i % 3 == 0:
        task = PythonOperator(
            task_id=f'fetch_url_{i}',
            python_callable=fetch_url,
            op_args=[i],
            dag=dag,
        )
    elif i % 3 == 1:
        task = PythonOperator(
            task_id=f'calculate_square_{i}',
            python_callable=calculate_square,
            op_args=[i],
            dag=dag,
        )
    else:
        task = PythonOperator(
            task_id=f'log_message_{i}',
            python_callable=log_message,
            op_args=[i],
            dag=dag,
        )
    tasks.append(task)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> tasks[0]
for i in range(5):
    tasks[i] >> tasks[i + 1]
tasks[-1] >> end
