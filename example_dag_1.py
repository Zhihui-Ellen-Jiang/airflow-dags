
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import time
import requests
from requests import RequestException


def create_dag(dag_id,
               schedule,
               dag_number,
               default_args):

    def benchmark(*args):
        url = "https://www.salesforce.com"
        try:
            print(f"Making a request to {url}")
            response = requests.get(url, timeout=10)  # Add a timeout to the request
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            print(f"Response status code: {response.status_code}")
            print(response.text[:200])  # Print the first 200 characters of the response text for brevity
        except Timeout:
            print(f"Request to {url} timed out.")
        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except ConnectionError:
            print(f"Failed to connect to {url}")
        except RequestException as req_err:
            print(f"Request failed: {req_err}")
        time.sleep(10)  # Reduced sleep time for local testing

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    start = DummyOperator(task_id='start', retries = 3, dag=dag)
    t1 = PythonOperator(
        task_id='benchmark_1',
        retries = 3,
        python_callable=benchmark,
        dag=dag)
    t2 = PythonOperator(
        task_id='benchmark_2',
        retries = 3,
        python_callable=benchmark,
        dag=dag)
    t3 = PythonOperator(
        task_id='benchmark_3',
        retries = 3,
        python_callable=benchmark,
        dag=dag)
    t4 = PythonOperator(
        task_id='benchmark_4',
        retries = 3,
        python_callable=benchmark,
        dag=dag)
    t5 = PythonOperator(
        task_id='benchmark_5',
        retries = 3,
        python_callable=benchmark,
        dag=dag)
    t6 = PythonOperator(
        task_id='benchmark_6',
        retries = 3,
        python_callable=benchmark,
        dag=dag)
    t7 = PythonOperator(
        task_id='benchmark_7',
        retries = 3,
        python_callable=benchmark,
        dag=dag)
    t8 = PythonOperator(
        task_id='benchmark_8',
        retries = 3,
        python_callable=benchmark,
        dag=dag)
    t9 = PythonOperator(
        task_id='benchmark_9',
        retries = 3,
        python_callable=benchmark,
        dag=dag)
    t10 = PythonOperator(
        task_id='benchmark_10',
        retries = 3,
        python_callable=benchmark,
        dag=dag)
    end = DummyOperator(task_id='end', retries = 3, dag=dag)
    start >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> end

    return dag
# build a dag for each number in range(10)
for n in range(1, 5):
    dag_id = 'benchmark_regular_{}'.format(str(n))

    default_args = {'owner': 'airflow',
                    'start_date': datetime(2022, 2, 22)
                    }

    schedule = None
    dag_number = n

    globals()[dag_id] = create_dag(dag_id,
                                  schedule,
                                  dag_number,
                                  default_args)
