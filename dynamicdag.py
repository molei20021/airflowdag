# coding: utf-8
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def create_dag(dag_id,
               schedule,
               dag_number,
               default_args):
    def hello_world_py(*args):
        print('Hello World')
        print('This is DAG: {}'.format(str(dag_number)))

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        t1 = PythonOperator(
            task_id='hello_world',
            python_callable=hello_world_py,
            dag_number=dag_number)
        t2 = BashOperator(
            task_id='current_date',
            bash_command='date'
        )
        t1 >> t2

    return dag


def get_api_data():
    data = ['test01', 'test02', 'test03']
    return data


def create_dags(data=None):
    for n in range(len(data)):
        dag_id = 'dynamic_day_{}'.format(data[n])
        default_args = {'owner': 'airflow',
                        'start_date': datetime(2019, 6, 1)}
        schedule = None
        dag_number = n
        globals()[dag_id] = create_dag(dag_id, schedule, dag_number, default_args)


data = get_api_data()
create_dags(data)