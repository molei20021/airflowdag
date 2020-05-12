import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.log.logging_mixin import LoggingMixin, StreamLogWriter, set_context
import logging

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

log = StreamLogWriter(logger=logging, level=logging.ERROR)


def doTestMysqlHook(*args, **kwargs):
    sql_hook = MySqlHook().get_hook(conn_id="mysql_operator_test_connid")
    sql = "select * from manzeng_predict_src_table;"
    result = sql_hook.get_records(sql)
    for row in result:
        print(row)
    sql = "select max(id) as max_id from manzeng_predict_src_table"
    result = sql_hook.get_records(sql)
    print('maxid:' + str(result[0][0]))
    result = sql_hook.get_first(sql)
    print('maxid:' + str(result[0]))
    log.write("err log test")
    sql_hook.run("""insert into manzeng_result_v3(consignor_phone,prediction) values('122','33')""")

doMysqlTask = PythonOperator(
    task_id='testmysqlhook',
    dag=dag,
    provide_context=True,
    python_callable=doTestMysqlHook,
    op_args=[],
)

doMysqlTask