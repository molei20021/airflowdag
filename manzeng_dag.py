# -*- coding: utf-8 -*-
from airflow import utils
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime, timedelta
import json
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'dag_concurrency': 50,
}

# schedule_interval: Seconds Minutes Hours DayofMonth Month DayofWeek Year
dag = DAG(
    "manzeng",
    default_args=default_args,
    description='manzeng',
    schedule_interval='0 0 8 2 * ? *',
)

task_trigger_data_prepare = SimpleHttpOperator(
    task_id='task_trigger_data_prepare',
    http_conn_id='task_proxy_comm',
    endpoint='bdptask/add',
    method='POST',
    headers={'Content-Type': 'application/json'},
    data={"bdp_task": {"sys_name": "task_manzeng","task_id": "12","content": "","trigger_url": "","parallel": ""}},
    dag=dag,
    retries=10,
    retry_delay=60,
    response_check=lambda response: True if 200 == response.status_code else False,
    xcom_push=True,
)

task_sense_data_prepare = HttpSensor(
    task_id='task_sense_data_prepare',
    http_conn_id='task_proxy_comm',
    method='GET',
    endpoint='bdptask/id',
    headers={'Content-Type': 'application/json'},
    request_params={"id": json.loads(json.loads(lambda context: context['task_instance'].xcom_pull(task_ids='task_trigger_data_prepare'))['bdp_task'])['id']},
    response_check=lambda response: True if 2 == json.loads(json.loads(response.text)['bdp_task'])['status'] else False,
    poke_interval=60,
)

task_trigger_py1_data_prepare = SimpleHttpOperator(
    task_id='task_trigger_py1_data_prepare',
    http_conn_id='task_proxy_comm',
    endpoint='bdptask/add',
    method='POST',
    headers={'Content-Type': 'application/json'},
    data={"bdp_task": {"sys_name": "task_manzeng", "task_id": "12", "content": "", "trigger_url": "", "parallel": ""}},
    dag=dag,
    retries=10,
    retry_delay=60,
    response_check=lambda response: True if 200 == response.status_code else False,
    xcom_push=True,
)

task_sense_py1_data_prepare = HttpSensor(
    task_id='task_sense_py1_data_prepare',
    http_conn_id='task_proxy_comm',
    method='GET',
    endpoint='bdptask/id',
    headers={'Content-Type': 'application/json'},
    request_params={"id":json.loads(json.loads(os.popen("echo {{ task_instance.xcom_pull(task_ids='task_trigger_py1_data_prepare') }}").read())['bdp_task'])['id']},
    response_check=lambda response: True if 2 == json.loads(json.loads(response.text)['bdp_task'])['status'] else False,
    poke_interval=60,
)

task_trigger_data_prepare >> task_sense_data_prepare >> task_trigger_py1_data_prepare >> task_sense_py1_data_prepare