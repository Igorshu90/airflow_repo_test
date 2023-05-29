"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.utils_main import test

local_tz = pendulum.timezone('Israel')

default_args = {
    "owner": "Igor3",
    "start_date": datetime(2015, 6, 1, tzinfo=local_tz)
}

with DAG(dag_id='igor_test_dag', catchup=False, default_args=default_args, schedule_interval='27 13 * * *') as dag:
    op1 = BashOperator(task_id='op1', bash_command='echo bla bla')
    op2 = PythonOperator(task_id='op2', python_callable=test)
    op3 = PythonOperator(task_id='op2', python_callable=test)

    op1 >> op2 >> op3