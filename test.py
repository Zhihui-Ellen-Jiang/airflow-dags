from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

# Define the function to hit the PostgreSQL database 100 times
def hit_postgresql_100_times(**kwargs):
    # Get the PostgreSQL connection from Airflow
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')  # Use the connection ID for Airflow's PostgreSQL database
    
    # Define a more complex SQL query
    query = """
    WITH RECURSIVE t(n) AS (
        VALUES (1)
        UNION ALL
        SELECT n+1 FROM t WHERE n < 1000
    )
    SELECT count(*) FROM t;
    """  # This will run a recursive query to create some load
    
    # Execute the query 100 times
    for i in range(100):
        pg_hook.run(query)
        print(f"Executed query {i + 1} times")

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
    'enhanced_single_task_hit_postgresql_100_times',
    default_args=default_args,
    description='A DAG to create a significant load on PostgreSQL',
    schedule_interval=None,  # No schedule, can be triggered manually
    catchup=False,
    max_active_runs=1,
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
