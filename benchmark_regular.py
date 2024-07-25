from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import time
import requests

def benchmark(*args):
    try:
        r = requests.get("https://www.salesforce.com", timeout=10)  # Add a timeout to the request
        print(r.text[:100])  # Print the first 100 characters for brevity
    except requests.RequestException as e:
        print(f"Request failed: {e}")
    time.sleep(60)  # Sleep for 60 seconds to simulate a delay

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 2, 22)
}

# Create a DAG
dag = DAG(
    'benchmark_regular',
    default_args=default_args,
    schedule_interval=None,  # No schedule, can be triggered manually
    catchup=False
)

# Create tasks
start = DummyOperator(task_id='start', dag=dag)
t1 = PythonOperator(task_id='benchmark_1', python_callable=benchmark, dag=dag)
t2 = PythonOperator(task_id='benchmark_2', python_callable=benchmark, dag=dag)
t3 = PythonOperator(task_id='benchmark_3', python_callable=benchmark, dag=dag)
t4 = PythonOperator(task_id='benchmark_4', python_callable=benchmark, dag=dag)
t5 = PythonOperator(task_id='benchmark_5', python_callable=benchmark, dag=dag)
t6 = PythonOperator(task_id='benchmark_6', python_callable=benchmark, dag=dag)
t7 = PythonOperator(task_id='benchmark_7', python_callable=benchmark, dag=dag)
t8 = PythonOperator(task_id='benchmark_8', python_callable=benchmark, dag=dag)
t9 = PythonOperator(task_id='benchmark_9', python_callable=benchmark, dag=dag)
t10 = PythonOperator(task_id='benchmark_10', python_callable=benchmark, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Set task dependencies
start >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> end
