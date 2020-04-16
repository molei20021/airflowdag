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

def doShapeReduce():
    print("doShapeReduce")

do_shape_reduce_task = PythonOperator(
    task_id='shape_reduce',
    dag=dag,
    provide_context=True,
    python_callable=doShapeReduce,
    op_args=[],
)

total = 5


for index in range(total):
    dynamicTask = PythonOperator(
        task_id='shape_map_' + index,
        dag=dag,
        provide_context=True,
        python_callable=doShapeMap,
        op_args=[index, index],
    )
    dynamicTask >> do_shape_reduce_task