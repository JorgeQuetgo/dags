from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime, timezone


def hola_mundo(**kwargs):
    print("hola Mundo")


default_args = {
    'owner': 'jorge.giron',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['jrgjit@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,    
    'retry_delay': timedelta(minutes=2),
    'schedule_interval': '@weekly',  
}


with DAG(dag_id='airflow_cleanup', catchup=False, 
         default_args=default_args) as airflow_cleanup: 

    task_hola = PythonOperator(
        task_id='task_hola',
        python_callable=hola_mundo,
        provide_context=True,        
        do_xcom_push=True,
    )


    task_hola 
