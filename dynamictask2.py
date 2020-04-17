import airflow
from airflow.operators.python_operator import PythonOperator

main_dag_id = 'DynamicTask2'

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True,
}

dag = airflow.DAG(
    main_dag_id,
    schedule_interval=None,
    default_args=args,
)

def doShapeMap(start, end, *args, **kwargs):
    print("doShapeMap:", start, " to ", end)

def doShapeReduce(*args, **kwargs):
    print("doShapeReduce")

do_shape_reduce_task = PythonOperator(
    task_id='shape_reduce',
    dag=dag,
    provide_context=True,
    python_callable=doShapeReduce,
    op_args=[],
)

def doTwoThree(start, end, *args, **kwargs):
    print("doTwoThree:", start, " to ", end)


total = 5


for index in range(total):
    dynamicShapeMapTask = PythonOperator(
        task_id='shape_map_' + str(index),
        dag=dag,
        provide_context=True,
        python_callable=doShapeMap,
        op_args=[index, index],
    )

    dynamicTwoThree_task = PythonOperator(
        task_id='two_three_' + str(index),
        dag=dag,
        provide_context=True,
        python_callable=doTwoThree,
        op_args=[index, index],
    )

    dynamicShapeMapTask >> do_shape_reduce_task >> dynamicTwoThree_task