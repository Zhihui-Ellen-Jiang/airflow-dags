from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
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
    time.sleep(15)  # Sleep for 15 seconds to simulate a delay

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
dag = DAG(
    'high_concurrency_benchmark',
    default_args=default_args,
    description='A DAG to create high number of concurrent benchmark tasks',
    schedule_interval=None,  # No schedule, can be triggered manually
    catchup=False,
    max_active_runs=1,
    concurrency=100  # Allow up to 100 tasks to run concurrently
)

# Create start task
start = DummyOperator(task_id='start', dag=dag)

# Create multiple benchmark tasks
benchmark_tasks = []
for i in range(1, 101):  # Create 100 tasks
    task = PythonOperator(
        task_id=f'benchmark_task_{i}',
        python_callable=benchmark,
        dag=dag
    )
    benchmark_tasks.append(task)

# Create end task
end = DummyOperator(task_id='end', dag=dag)

# Set task dependencies
start >> benchmark_tasks >> end

# Optionally, you can set dependencies in a different pattern if needed
# for i in range(len(benchmark_tasks) - 1):
#     benchmark_tasks[i] >> benchmark_tasks[i + 1]
