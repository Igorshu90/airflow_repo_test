import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.mssql_to_gcs import MSSQLToGCSOperator
from datetime import date
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2015, 6, 1),
#     "email": ["airflow@airflow.com"],
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5)
#
# }
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
today = date.today()

dag = DAG("mrr2", default_args=default_args, schedule_interval=timedelta(1))

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
    table_json = json.load(file)

for index, query in enumerate(table_json):
    export_customers = MSSQLToGCSOperator(
        task_id=f'load_to_GSC_' + query['table'],
        sql=query['query'].format(interval='day', offset='-1'),
        bucket='igor_airflow_dev',
        filename=query['table'],
        mssql_conn_id='mssql_default',
        gcp_conn_id='google_cloud_default',
        export_format='csv',
        dag=dag
    )
    load_to_bq = GCSToBigQueryOperator(
        task_id=f'load_to_bq_' + query['table'],
        bucket='igor_airflow_dev',
        source_objects=query['table'],
        destination_project_dataset_table=f'igordwh.MRR.{query["table"]}',
        source_format='csv',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='google_cloud_default',
        dag=dag
    )

    start >> export_customers >> load_to_bq >> end
