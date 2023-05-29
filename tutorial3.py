"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from google.oauth2 import service_account
from airflow.models import Variable
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)

}


def mssql_func(**kwargs):
    path = Variable.get('gcp_credentials_path')
    project_id = Variable.get('project_id')
    credentials = service_account.Credentials.from_service_account_file(path)
    hook = MsSqlHook(conn_id='mssql_default')
    df = hook.get_pandas_df(sql=kwargs['sql'],)
    destination = kwargs['destination']
    df = df.astype(str)
    df.to_gbq(destination_table=f'MRR.{destination}', credentials=credentials, project_id=project_id,
              if_exists='replace')


dag = DAG("tutorial3", default_args=default_args, schedule_interval=timedelta(1))

start = BashOperator(
    task_id='start',
    bash_command='echo start',
    dag=dag,
)

end = BashOperator(
    task_id='end',
    bash_command='echo end',
    dag=dag,
)

with open('/home/igor/airflow/dags/mrr_querys.json', 'r') as file:
    json_table = json.load(file)

for query in json_table:
    mrr_task = PythonOperator(
        task_id=query['table'],
        python_callable=mssql_func,
        op_kwargs={'sql': query['query'], 'destination': query['destination']},
        dag=dag
    )

    start >> mrr_task >> end
