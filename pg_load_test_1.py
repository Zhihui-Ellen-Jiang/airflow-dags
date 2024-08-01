from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import concurrent.futures

# Define the function to hit the PostgreSQL database 100 times with complex queries
def complex_algorithm(**kwargs):
    # Get the PostgreSQL connection from Airflow
    pg_hook = PostgresHook(postgres_conn_id='pgbouncer_default')
    
    # Define a list of meaningful SQL queries
    queries = [
        """
        WITH RECURSIVE t(n) AS (
            SELECT 1
            UNION ALL
            SELECT n + 1 FROM t WHERE n < 1000
        )
        SELECT COUNT(*), SUM(n), AVG(n), MAX(n), MIN(n) FROM t;
        """,
        """
        WITH RECURSIVE t(n) AS (
            SELECT 1
            UNION ALL
            SELECT n + 1 FROM t WHERE n < 1000
        )
        SELECT n, pg_sleep(0.01) FROM t ORDER BY n DESC;
        """,
        """
        SELECT generate_series(1, 1000) AS series, md5(random()::text) 
        FROM generate_series(1, 1000)
        JOIN (SELECT generate_series(1, 1000) AS series2) AS t2 ON series = t2.series2;
        """,
        """
        SELECT COUNT(*), AVG(duration), MAX(duration), MIN(duration), STDDEV(duration) 
        FROM task_instance 
        WHERE execution_date > now() - interval '1 day';
        """,
        """
        SELECT c.*, t1.*, t2.*
        FROM connection c
        JOIN task_instance t1 ON c.connection_id = t1.connection_id
        JOIN dag_run t2 ON t1.dag_id = t2.dag_id
        ORDER BY c.connection_id DESC LIMIT 50;
        """,
        """
        SELECT task_id, COUNT(*), AVG(duration), STDDEV(duration)
        FROM task_instance
        GROUP BY task_id
        HAVING COUNT(*) > 5
        ORDER BY AVG(duration) DESC LIMIT 10;
        """,
        """
        SELECT dag_id, COUNT(*), MAX(duration), MIN(duration), AVG(duration), STDDEV(duration)
        FROM task_instance
        GROUP BY dag_id
        HAVING COUNT(*) > 10
        ORDER BY AVG(duration) DESC;
        """,
        """
        SELECT * 
        FROM connection 
        WHERE connection_id IN (
            SELECT connection_id 
            FROM connection 
            ORDER BY connection_id DESC LIMIT 10
        );
        """,
        """
        WITH RECURSIVE cte AS (
            SELECT 1 AS n
            UNION ALL
            SELECT n + 1 FROM cte WHERE n < 1000
        )
        SELECT n, pg_sleep(0.01), md5(random()::text)
        FROM cte
        ORDER BY n DESC;
        """,
        """
        DO $$ 
        BEGIN 
            PERFORM pg_advisory_lock(1);
            PERFORM pg_sleep(0.5);
            PERFORM pg_advisory_unlock(1);
        END $$;
        """
    ] * 500  # Repeat the list to make it 1

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
    'pg_load_test_1',
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
