import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

main_dag_id = 'DynamicTask3'

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(90),
    'provide_context': True,
}

dag = airflow.DAG(
    main_dag_id,
    schedule_interval='0 11 22 * *',
    default_args=args,
)


def doTwoThreeReduce(*args, **kwargs):
    print("doTwoThreeReduce")

doTwoThreeReduceTask = PythonOperator(
    task_id='two_three_reduce',
    dag=dag,
    provide_context=True,
    python_callable=doTwoThreeReduce,
    op_args=[],
)

def doShapeMap(start, end, *args, **kwargs):
    doDynamicShapeMapTask >> doTwoThreeReduceTask

doDynamicShapeMapTask = PythonOperator(
    task_id='shape_map_' + str(1),
    dag=dag,
    provide_context=True,
    python_callable=doShapeMap,
    op_args=[1, 1],
)

doDynamicShapeMapTask