import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

local_tz = pendulum.timezone('Israel')

default_args = {
    "owner": "Igor",
    "start_date": datetime(2015, 6, 1, tzinfo=local_tz)
}


def branch(**kwargs):
    if kwargs['option'] == 1:
        return 'op1'
    elif kwargs['option'] == 2:
        return 'op2'
    elif kwargs['option'] == 3:
        return 'op3'
    else:
        raise ValueError('invalid option')


with DAG(dag_id='igor_test_dag3', catchup=False, default_args=default_args, schedule_interval='27 13 * * *') as dag:
    start = BashOperator(task_id='start', bash_command='echo start')
    end = BashOperator(task_id='end', bash_command='echo end', trigger_rule='one_success')
    op1 = BashOperator(task_id='op1', bash_command='echo bla')
    op2 = BashOperator(task_id='op2', bash_command='echo bla bla')
    op3 = BashOperator(task_id='op3', bash_command='echo bla bla bla')
    branch_op = BranchPythonOperator(task_id='branch_task', python_callable=branch, op_kwargs={'option': 1})
    start >> branch_op >> [op1, op2, op3] >> end
