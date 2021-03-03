# airflow related
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

# other packages
from datetime import datetime, timedelta

# import operators from the 'operators' file
from glo_r20 import glo_r20_function
from glo_r21 import glo_r21_function
from glo_r22 import glo_r22_function
from glo_r24 import glo_r24_function
from glo_r25 import glo_r25_function
from glo_r26 import glo_r26_function
from glo_r28 import glo_r28_function
from glo_r29 import glo_r29_function
from glo_r34 import glo_r34_function
from glo_r35 import glo_r35_function


# import lithops function
#from jti_lithops_function import jti_lithops_function 

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("pdbms3_dag", default_args=default_args, schedule_interval=None)

glo_r20 = PythonOperator(
    task_id='glo_r20',
    python_callable=glo_r20_function,
    op_args=['glo_r20'],
    dag=dag)

glo_r21 = PythonOperator(
    task_id='glo_r21',
    python_callable=glo_r21_function,
    op_args=['glo_r21'],
    dag=dag)

glo_r22 = PythonOperator(
    task_id='glo_r22',
    python_callable=glo_r22_function,
    op_args=['glo_r22'],
    dag=dag)

glo_r24 = PythonOperator(
    task_id='glo_r24',
    python_callable=glo_r24_function,
    op_args=['glo_r24'],
    dag=dag)

glo_r25 = PythonOperator(
    task_id='glo_r25',
    python_callable=glo_r25_function,
    op_args=['glo_r25'],
    dag=dag)

glo_r26 = PythonOperator(
    task_id='glo_r26',
    python_callable=glo_r26_function,
    op_args=['glo_r26'],
    dag=dag)

glo_r28 = PythonOperator(
    task_id='glo_r28',
    python_callable=glo_r28_function,
    op_args=['glo_r28'],
    dag=dag)

glo_r29 = PythonOperator(
    task_id='glo_r29',
    python_callable=glo_r29_function,
    op_args=['glo_r29'],
    dag=dag)
    
glo_r34 = PythonOperator(
    task_id='glo_r34',
    python_callable=glo_r34_function,
    op_args=['glo_r34'],
    dag=dag)

glo_r35 = PythonOperator(
    task_id='glo_r35',
    python_callable=glo_r35_function,
    op_args=['glo_r35'],
    dag=dag)

#DAG Sequences
glo_r20 >> glo_r21 >> glo_r22 >> glo_r24 >> glo_r25 >> glo_r26 >> glo_r28 >> glo_r29 >> glo_r34 >> glo_r35