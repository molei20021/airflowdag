# Filename: hello_main_dag.py
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
import sys,os
curPath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(curPath)
from hello_sub_dag import my_sub_dag

# Step 1 - define the default parameters for the DAG
default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2019, 7, 28),
  'email': ['vipin.chadha@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),

}

# Step 2 - Create a DAG object
dag = DAG(dag_id='hello_main_dag',
        schedule_interval='0 0 * * *' ,
        default_args=default_args
    )

# Step 3 - Create tasks
some_task = BashOperator(
  task_id= 'someTask',
  bash_command="echo I am some task",
  dag=dag
)

sub_dag = SubDagOperator(
    subdag=my_sub_dag('hello_main_dag'),
    task_id='my_sub_dag',
    dag=dag
)

final_task = BashOperator(
  task_id= 'finalTask',
  bash_command="echo I am the final task",
  dag=dag
)

# Step 4 - Define the sequence of tasks.
sub_dag.set_upstream(some_task)
final_task.set_upstream(sub_dag)