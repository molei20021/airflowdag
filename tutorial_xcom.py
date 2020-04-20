# -*- coding: utf-8 -*-
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'xcomtest',
    default_args=default_args,
    description='xcomtest',
    schedule_interval="@once",
)


# 注入 **kwargs ，从**kwargs 中获取外界传来的数据；
def dateEnd(ds, **kwargs):
    return kwargs['dag_run'].conf['date_end']


def dateStart(ds, **kwargs):
    r = requests.get('https://api.github.com/events')
    print(r.content)
    return kwargs['dag_run'].conf['date_start']


def getMacLists(ds, **kwargs):
    businessid = kwargs['dag_run'].conf['businessid']
    shopid = kwargs['dag_run'].conf['shopid']
    mac = kwargs['dag_run'].conf['mac']
    return "getMacLists_result"


t1 = PythonOperator(
    task_id='dateStart',
    provide_context=True,
    python_callable=dateStart,
    dag=dag)
t2 = PythonOperator(
    task_id='dateEnd',
    provide_context=True,
    python_callable=dateEnd,
    dag=dag)
t3 = PythonOperator(
    task_id='getMacLists',
    provide_context=True,
    python_callable=getMacLists,
    dag=dag)
t4 = BashOperator(
    task_id='xcomview',
    bash_command="echo {{ task_instance.xcom_pull(task_ids='getMacLists') }} {{ task_instance.xcom_pull(task_ids='dateStart') }} {{ task_instance.xcom_pull(task_ids='dateEnd') }} ",
    dag=dag,
)
[t1, t2, t3] >> t4
