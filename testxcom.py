from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

DAG = DAG(
  dag_id='example_dag',
  start_date=datetime.now(),
  schedule_interval='@once'
)

def push_function(**kwargs):
    result = {'ortho': 's3://pwlab-dataset/output/2021-04-22 17:00:48/orthophoto.tif', 'photogrametry_result': 's3://pwlab-dataset/output/2021-04-22 17:00:48/all.zip'}
    return result

push_task = PythonOperator(
    task_id='push_task', 
    python_callable=push_function,
    provide_context=True,
    dag=DAG)

def pull_function(**kwargs):
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='push_task')
    print(ls)
    print("ortho",ls.get('ortho'))

pull_task = PythonOperator(
    task_id='pull_task', 
    python_callable=pull_function,
    provide_context=True,
    dag=DAG)

push_task >> pull_task
