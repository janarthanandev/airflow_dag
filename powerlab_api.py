# -*- coding: utf-8 -*-

from __future__ import print_function

import time
from builtins import range
from pprint import pprint
import json
import logging
import requests
from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

#Set Log Level
logging.basicConfig(level=logging.INFO)

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='powerlab__pipeline',
    default_args=args,
    schedule_interval=None,
    tags=['pwlab']
)


def blur_detection(ds, **kwargs):
    dqurl = "http://35.174.137.100:32060/dq"
    payload = {
        "datasource_url" : "s3://pwlab-dataset",
        "threshold" : 100
	}
    headers = {
        "Content-Type" : "application/json"
    }
    response = requests.request("POST", dqurl, headers=headers, data=json.dumps(payload))
    res = json.loads(response.text).get('results')
    status = json.loads(response.text).get('status')
    logging.info(status)
    logging.info(res)
    pprint(kwargs)
    print(ds)
    return 'blur check passed'

def reflectance(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'reflectance check passed'

def img_metadata_analysis(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'img_metadata_analysis check passed'


dq_check_start = BashOperator(
    task_id='dq_check',
    bash_command='echo "DQ check"',
    dag=dag,
)

dq_check_blur_detection = PythonOperator(
    task_id='blur_detection',
    provide_context=True,
    python_callable=blur_detection,
    dag=dag,
)
dq_check_reflectance = PythonOperator(
    task_id='reflectance',
    provide_context=True,
    python_callable=reflectance,
    dag=dag,
)
dq_check_img_metadata_analysis = PythonOperator(
    task_id='img_metadata_analysis',
    provide_context=True,
    python_callable=img_metadata_analysis,
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

def object_count():
    return "object_count done"

get_object_count = PythonOperator(
    task_id='object_count',
    provide_context=True,
    python_callable=object_count,
    dag=dag,
)

# dq_check >> blur_detection >> reflectance >> img_metadata_analysis
dq_check_start >> [dq_check_blur_detection, dq_check_reflectance, dq_check_img_metadata_analysis] >> photogrametry >> get_object_count
