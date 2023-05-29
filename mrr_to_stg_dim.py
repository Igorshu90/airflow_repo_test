import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator



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


# default_args = {
#     "owner": "airflow",
#     "start_date": datetime(2015, 6, 1),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5)

#}

today = date.today()

dag = DAG("mrr_to_stg_dim2", default_args=default_args)

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

with open('/home/igor/airflow/dags/mrr_to_stg_dim_querys.json', 'r') as file:
    table_json = json.load(file)

for index, query in enumerate(table_json):
    mrr_to_stg = BigQueryInsertJobOperator(
        task_id=f"load_from{query['source_table']}_to_{query['distension_table']}",
        configuration={
            'query': {
                'query': query['query'],
                "writeDisposition": "WRITE_TRUNCATE",
                'destinationTable': {
                    'projectId': 'igordwh',
                    'datasetId': 'STG',
                    'tableId': query['distension_table'],

                },
                'useLegacySql': False,
                'allowLargeResults': True,
            }
        },
    #    write_dispotition='WRITE_TRUNCATE',
        dag=dag
    )

    start >> mrr_to_stg >> end
