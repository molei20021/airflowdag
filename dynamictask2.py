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

doShapeReduceTask = PythonOperator(
    task_id='shape_reduce',
    dag=dag,
    provide_context=True,
    python_callable=doShapeReduce,
    op_args=[],
)

def doTwoThree(start, end, *args, **kwargs):
    print("doTwoThree:", start, " to ", end)


def doTwoThreeReduce(*args, **kwargs):
    print("doTwoThreeReduce")

doTwoThreeReduceTask = PythonOperator(
    task_id='two_three_reduce',
    dag=dag,
    provide_context=True,
    python_callable=doTwoThreeReduce,
    op_args=[],
)

total = 5


for index in range(total):
    doDynamicShapeMapTask = PythonOperator(
        task_id='shape_map_' + str(index),
        dag=dag,
        provide_context=True,
        python_callable=doShapeMap,
        op_args=[index, index],
    )

    doDynamicTwoThreeTask = PythonOperator(
        task_id='two_three_' + str(index),
        dag=dag,
        provide_context=True,
        python_callable=doTwoThree,
        op_args=[index, index],
    )

    doDynamicShapeMapTask >> doShapeReduceTask >> doDynamicTwoThreeTask >> doTwoThreeReduceTask