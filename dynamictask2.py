import airflow
from airflow.operators.python_operator import PythonOperator
import time

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

currtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def doShapeMap(start, end, *args, **kwargs):
    print("doShapeMap:", start, " to ", end)
    print(currtime)

def doShapeReduce(*args, **kwargs):
    print("doShapeReduce")
    print(currtime)
    fw = open('/tmp/shapetest.txt', 'w')
    fw.write(currtime)
    fw.close()

doShapeReduceTask = PythonOperator(
    task_id='shape_reduce',
    dag=dag,
    provide_context=True,
    python_callable=doShapeReduce,
    op_args=[],
)

def doTwoThree(start, end, *args, **kwargs):
    currtime_tmp = ''
    with open('/tmp/shapetest.txt', 'r') as fr:
        currtime_tmp = fr.read()
    print("doTwoThree:", start, " to ", end)
    print(currtime_tmp)


def doTwoThreeReduce(*args, **kwargs):
    currtime_tmp = ''
    with open('/tmp/shapetest.txt', 'r') as fr:
        currtime_tmp = fr.read()
    print("doTwoThreeReduce:%s" % currtime_tmp)

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