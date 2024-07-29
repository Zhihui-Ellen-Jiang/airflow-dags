from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator
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
    dag_id='test_postgres_connection',
    default_args=default_args,
    description='A simple DAG to test PostgreSQL connection',
    schedule_interval=None,  # No schedule, can be triggered manually
    catchup=False
)

# Create start task
start = DummyOperator(task_id='start', dag=dag)

# Define a simple SQL query
query = "SELECT 1;"

# Create Postgres task
postgres_task = SQLExecuteQueryOperator(
    task_id='test_postgres_query',
    conn_id='postgres_default',  # Connection ID set up in Airflow
    sql=query,
    dag=dag
)

# Create end task
end = DummyOperator(task_id='end', dag=dag)

# Set task dependencies
start >> postgres_task >> end
