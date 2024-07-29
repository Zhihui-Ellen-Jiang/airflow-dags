from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import time
import random

# Function to simulate a task
def parallel_task(task_number):
    time_to_sleep = random.randint(1, 10)
    print(f"Task {task_number} is sleeping for {time_to_sleep} seconds.")
    time.sleep(time_to_sleep)
    print(f"Task {task_number} is done.")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='parallel_tasks_dag',
    default_args=default_args,
    schedule_interval=None,  # Set to None for manual trigger
    catchup=False,
) as dag:

    # Define the TaskGroup
    with TaskGroup("parallel_tasks_group") as parallel_tasks_group:
        # Define the parallel tasks within the TaskGroup
        for i in range(5):  # Adjust the range for the desired number of parallel tasks
            task = PythonOperator(
                task_id=f'parallel_task_{i}',
                python_callable=parallel_task,
                op_args=[i],
            )

    # Define the single task that executes the TaskGroup
    single_task = PythonOperator(
        task_id='single_task',
        python_callable=lambda: print("Single task executed."),
    )

    # Define the DAG structure
    single_task >> parallel_tasks_group

if __name__ == "__main__":
    dag.cli()
