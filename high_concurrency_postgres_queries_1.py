from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a single DAG
dag = DAG(
    'high_concurrency_postgres_queries_1',
    default_args=default_args,
    description='A DAG to create high number of PostgreSQL connections concurrently',
    schedule_interval=None,  # No schedule, can be triggered manually
    catchup=False,
    max_active_runs=1,
    concurrency=100  # Allow up to 100 tasks to run concurrently
)

# Create start task
start = DummyOperator(task_id='start', dag=dag)

# Define a simple SQL query
query = "SELECT pg_sleep(5);"  # This will hold the connection open for 5 seconds

# Create multiple Postgres tasks
postgres_tasks = []
for i in range(1, 101):
    task = PostgresOperator(
        task_id=f'postgres_query_{i}',
        postgres_conn_id='postgres_default',  # Connection ID set up in Airflow
        sql=query,
        dag=dag
    )
    postgres_tasks.append(task)

# Create end task
end = DummyOperator(task_id='end', dag=dag)

# Set task dependencies
start >> postgres_tasks >> end
