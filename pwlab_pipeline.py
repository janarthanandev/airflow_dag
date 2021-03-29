# -*- coding: utf-8 -*-

from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='powerlab_sample_pipeline',
    default_args=args,
    schedule_interval=None,
    tags=['pwlab']
)


def blur_detection(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'blur check passed'


dq_check = PythonOperator(
    task_id='blur_detection',
    provide_context=True,
    python_callable=blur_detection,
    dag=dag,
)

def photogrametry_trigger():
    return "photogrametry done"


photogrametry = PythonOperator(
    task_id='photogrametry',
    provide_context=True,
    python_callable=photogrametry_trigger,
    dag=dag,
)

dq_check >> photogrametry
