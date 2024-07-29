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
    'retry_delay': timedelta(minutes=1),
}

# Create a single DAG
dag = DAG(
    dag_id='concurrent_postgres_queries_2',
    default_args=default_args,
    description='A DAG to run multiple PostgreSQL queries on Airflow metadata database concurrently',
    schedule_interval=None,  # No schedule, can be triggered manually
    max_active_runs=1,
    catchup=False,
    concurrency=20  # Allow up to 20 tasks to run concurrently
)

# Create start task
start = DummyOperator(task_id='start', dag=dag)

# Define SQL queries for the Airflow metadata database
queries = [
    "SELECT COUNT(*) FROM dag;",
    "SELECT AVG(duration) FROM task_instance WHERE duration IS NOT NULL;",
    "SELECT MAX(start_date) FROM task_instance;",
    "SELECT MIN(end_date) FROM task_instance;",
    "SELECT * FROM connection LIMIT 10;",
    "SELECT MIN(end_date) FROM task_instance;",
    "SELECT COUNT(*) FROM dag;",
    "SELECT * FROM connection LIMIT 10;",
    "SELECT * FROM connection LIMIT 10;",
    "SELECT * FROM connection LIMIT 10;",
    "SELECT COUNT(*) FROM dag;",
    "SELECT AVG(duration) FROM task_instance WHERE duration IS NOT NULL;",
    "SELECT MAX(start_date) FROM task_instance;",
    "SELECT MIN(end_date) FROM task_instance;",
    "SELECT * FROM connection LIMIT 10;",
    "SELECT MIN(end_date) FROM task_instance;",
    "SELECT COUNT(*) FROM dag;",
    "SELECT * FROM connection LIMIT 10;",
    "SELECT * FROM connection LIMIT 10;",
    "SELECT * FROM connection LIMIT 10;"
]

# Create Postgres tasks
postgres_tasks = []
for i, query in enumerate(queries):
    task = PostgresOperator(
        task_id=f'postgres_query_{i + 1}',
        postgres_conn_id='postgres_default',
        sql=query,
        dag=dag
    )
    postgres_tasks.append(task)

# Create end task
end = DummyOperator(task_id='end', dag=dag)

# Set task dependencies
start >> postgres_tasks  # All Postgres tasks start after the start task completes
postgres_tasks >> end  # The end task starts after all Postgres tasks complete
