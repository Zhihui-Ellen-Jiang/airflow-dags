from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import time


# Define the benchmark function
def benchmark(*args):
    def fibonacci(n):
        if n <= 1:
            return n
        else:
            return fibonacci(n - 1) + fibonacci(n - 2)

    result = fibonacci(30)  # Adjust the number for a reasonable workload
    print(f"Fibonacci result: {result}")
    time.sleep(60)  # Sleep for 60 seconds to simulate a delay


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create a single DAG
dag_id = 'benchmark_regular'
dag = DAG(
    dag_id,
    default_args=default_args,
    description='A simple DAG with benchmark tasks',
    schedule_interval=None,  # No schedule, can be triggered manually
    max_active_runs=1
)

# Create tasks
start = DummyOperator(task_id='start', dag=dag)

benchmark_tasks = []
for i in range(1, 11):
    task = PythonOperator(
        task_id=f'benchmark_{i}',
        python_callable=benchmark,
        dag=dag
    )
    benchmark_tasks.append(task)

end = DummyOperator(task_id='end', dag=dag)

# Set task dependencies
start >> benchmark_tasks[0]
for i in range(len(benchmark_tasks) - 1):
    benchmark_tasks[i] >> benchmark_tasks[i + 1]
benchmark_tasks[-1] >> end
