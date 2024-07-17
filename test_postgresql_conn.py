from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'test_postgres_connection',
    default_args=default_args,
    description='A simple DAG to test PostgreSQL connection',
    schedule_interval=None,
)

# Task to create a test table
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='my_postgres_conn',
    sql='''
    CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL
    );
    ''',
    dag=dag,
)

# Task to insert data into the test table
insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='my_postgres_conn',
    sql='''
    INSERT INTO test_table (name) VALUES ('test_name');
    ''',
    dag=dag,
)

# Task to select data from the test table
select_data = PostgresOperator(
    task_id='select_data',
    postgres_conn_id='my_postgres_conn',
    sql='SELECT * FROM test_table;',
    dag=dag,
)

create_table >> insert_data >> select_data
