import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

main_dag_id = 'mysqlhook'
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

def doTestMysqlHook(*args, **kwargs):
    sql_hook = MySqlHook().get_hook(conn_id="mysql_operator_test_connid")
    sql = "select * from manzeng_predict_src_table;"
    result = sql_hook.get_records(sql)
    for row in result:
        print(row)

doMysqlTask = PythonOperator(
    task_id='testmysqlhook',
    dag=dag,
    provide_context=True,
    python_callable=doTestMysqlHook,
    op_args=[],
)

doMysqlTask