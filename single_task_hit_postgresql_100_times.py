from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

# Define the function to hit the PostgreSQL database 100 times
def hit_postgresql_100_times(**kwargs):
    # Get the PostgreSQL connection from Airflow
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Define a list of meaningful SQL queries
    queries = [
        "SELECT MIN(end_date) FROM task_instance;",
        "SELECT * FROM connection LIMIT 10;",
        "SELECT MIN(end_date) FROM task_instance;",
        "SELECT COUNT(*) FROM dag;",
        "SELECT AVG(duration) FROM task_instance WHERE duration IS NOT NULL;",
        "SELECT MAX(start_date) FROM task_instance;",
        "SELECT MIN(end_date) FROM task_instance;",
        "SELECT * FROM connection LIMIT 10;",
        "SELECT MIN(end_date) FROM task_instance;",
        "SELECT COUNT(*) FROM dag;"
    ] * 100  # Repeat the list to make it 1000 queries

    # Function to run a single query
    def run_query(query):
        pg_hook.run(query)
        print(f"Executed query: {query}")

    # Use ThreadPoolExecutor to run queries concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        executor.map(run_query, queries)


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

# Create the DAG
dag = DAG(
    'single_task_hit_postgresql_100_times',
    default_args=default_args,
    description='A DAG with one task hitting PostgreSQL 100 times',
    schedule_interval=None,  # No schedule, can be triggered manually
    catchup=False,
)

# Create start and end tasks
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Create the main task
hit_postgresql_task = PythonOperator(
    task_id='hit_postgresql_100_times',
    python_callable=hit_postgresql_100_times,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
start >> hit_postgresql_task >> end
