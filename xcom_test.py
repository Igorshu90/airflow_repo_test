import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import macros

local_tz = pendulum.timezone('Israel')

default_args = {
    "owner": "Igor",
    "start_date": datetime(2015, 6, 1, tzinfo=local_tz)
}


def push_xcom(ti):
    ti.xcom_push(key='ig_test', value=f'blabla {{ds}}')


def pull_xcom(ti, **kwargs):
    print('******************************************************************************************')
    print(ti.xcom_pull(key='ig_test', task_ids=['push_xcom_text']))
    print(str(kwargs['ds']))
    print('******************************************************************************************')


def op_args_test(a, b, c):
    print(f'!!**************{a}*******************!!')


with DAG(dag_id='xcom_test', catchup=False, default_args=default_args, schedule_interval='27 13 * * *') as dag:
    push_xcom_text = PythonOperator(
        task_id='push_xcom_text',
        python_callable=push_xcom,
        provide_context=True
    )
    pull_xcom_text = PythonOperator(
        task_id='pull_xcom_text',
        python_callable=pull_xcom,
        provide_context=True,
        op_kwargs={'current_date': "{{ds}}"
                   },
        params={'bla': 1}
    )
    op_args_test = PythonOperator(
        task_id='op_args_test',
        python_callable=op_args_test,
        op_kwargs={'b': 'b', 'a': 'a'}
    )
    push_xcom_text >> pull_xcom_text >> op_args_test
