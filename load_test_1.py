from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import concurrent.futures

# Define the function to hit the PostgreSQL database 100 times with complex queries
def complex_algorithm(**kwargs):
    # Get the PostgreSQL connection from Airflow
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Define a list of meaningful SQL queries
    queries = [
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 1000) SELECT COUNT(*) FROM t;",
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 1000) SELECT n, pg_sleep(0.01) FROM t;",
        "SELECT generate_series(1, 1000) AS series;",
        "SELECT COUNT(*), AVG(duration), MAX(duration), MIN(duration) FROM task_instance;",
        "SELECT * FROM connection ORDER BY connection_id DESC LIMIT 50;",
        "SELECT task_id, COUNT(*), AVG(duration) FROM task_instance GROUP BY task_id ORDER BY AVG(duration) DESC LIMIT 10;",
        "SELECT dag_id, COUNT(*), MAX(duration) FROM task_instance GROUP BY dag_id HAVING COUNT(*) > 10;",
        "SELECT * FROM connection WHERE connection_id IN (SELECT connection_id FROM connection ORDER BY connection_id DESC LIMIT 10);",
        "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 1000) SELECT n, pg_sleep(0.01) FROM cte;",
        "SELECT pg_advisory_lock(1); SELECT pg_advisory_unlock(1);"
    ] * 500 # Repeat the list to make it 100 queries

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
    'concurrent_queries_hit_postgresql_100_times_1',
    default_args=default_args,
    description='A DAG with one task hitting PostgreSQL 100 times with concurrent queries',
    schedule_interval=None,  # No schedule, can be triggered manually
    catchup=False,
    max_active_runs=1,
    concurrency=100  # Allow up to 100 tasks to run concurrently
)

# Create start and end tasks
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Create the main task
hit_postgresql_task = PythonOperator(
    task_id='hit_postgresql_100_times',
    python_callable=complex_algorithm,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
start >> hit_postgresql_task >> end
