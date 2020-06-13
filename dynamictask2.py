import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
import time
import os

BdpHttpConn = 'bdp_conn'
BdpHttpConnTriggerJobEndPoint = 'schedule-control/task/triggerJob'

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

def save_curr_time():
    fw = open('/tmp/shapetest.txt', 'w')
    fw.write(currtime)
    fw.close()
def get_curr_time_from_file():
    if False == os.path.exists('/tmp/shapetest.txt'):
        save_curr_time()
    with open('/tmp/shapetest.txt', 'r') as fr:
        currtime_tmp = fr.read()
    return currtime_tmp

def doShapeMap(start, end, *args, **kwargs):
    print("doShapeMap:", start, " to ", end)
    print(currtime)

def doShapeReduce(*args, **kwargs):
    print("doShapeReduce")
    print(currtime)
    save_curr_time()

doShapeReduceTask = PythonOperator(
    task_id='shape_reduce',
    dag=dag,
    provide_context=True,
    python_callable=doShapeReduce,
    op_args=[],
)

def doTwoThree(start, end, *args, **kwargs):
    print("doTwoThree:", start, " to ", end)
    print(get_curr_time_from_file())


def doTwoThreeReduce(*args, **kwargs):
    print("doTwoThreeReduce:%s" % get_curr_time_from_file())

doTwoThreeReduceTask = PythonOperator(
    task_id='two_three_reduce',
    dag=dag,
    provide_context=True,
    python_callable=doTwoThreeReduce,
    op_args=[],
)

total = 5

task_trigger_data_prepare_step_one = SimpleHttpOperator(
    task_id='task_trigger_data_prepare_step_one',
    http_conn_id=BdpHttpConn,
    endpoint=BdpHttpConnTriggerJobEndPoint,
    method='GET',
    headers={'Content-Type': 'application/json'},
    data={"curr_time": get_curr_time_from_file()},
    dag=dag,
    retries=10,
    retry_delay=60,
    response_check=lambda response: True if 200 == response.status_code else False,
    xcom_push=True,
)

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

    doDynamicShapeMapTask >> doShapeReduceTask >> doDynamicTwoThreeTask >> doTwoThreeReduceTask >> task_trigger_data_prepare_step_one