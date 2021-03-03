# airflow related
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

# other packages
from datetime import datetime, timedelta

# import operators from the 'operators' file
from glo_r39 import glo_r39_function
from glo_r42 import glo_r42_function
from glo_r60 import glo_r60_function
from glo_r67 import glo_r67_function
from glo_r70 import glo_r70_function
from glo_r93 import glo_r93_function
from glo_r94 import glo_r94_function
from glo_ra3 import glo_ra3_function
from glo_ra4 import glo_ra4_function
from glo_ra5 import glo_ra5_function


# import lithops function
#from jti_lithops_function import jti_lithops_function 

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("pdbms4_dag", default_args=default_args, schedule_interval=None)

glo_r39 = PythonOperator(
    task_id='glo_r39',
    python_callable=glo_r39_function,
    op_args=['glo_r39'],
    dag=dag)

glo_r42 = PythonOperator(
    task_id='glo_r42',
    python_callable=glo_r42_function,
    op_args=['glo_r42'],
    dag=dag)

glo_r60 = PythonOperator(
    task_id='glo_r60',
    python_callable=glo_r60_function,
    op_args=['glo_r60'],
    dag=dag)

glo_r67 = PythonOperator(
    task_id='glo_r67',
    python_callable=glo_r67_function,
    op_args=['glo_r67'],
    dag=dag)

glo_r70 = PythonOperator(
    task_id='glo_r70',
    python_callable=glo_r70_function,
    op_args=['glo_r70'],
    dag=dag)

glo_r93 = PythonOperator(
    task_id='glo_r93',
    python_callable=glo_r93_function,
    op_args=['glo_r93'],
    dag=dag)

glo_r94 = PythonOperator(
    task_id='glo_r94',
    python_callable=glo_r94_function,
    op_args=['glo_r94'],
    dag=dag)

glo_ra3 = PythonOperator(
    task_id='glo_ra3',
    python_callable=glo_ra3_function,
    op_args=['glo_ra3'],
    dag=dag)
    
glo_ra4 = PythonOperator(
    task_id='glo_ra4',
    python_callable=glo_ra4_function,
    op_args=['glo_ra4'],
    dag=dag)

glo_ra5 = PythonOperator(
    task_id='glo_ra5',
    python_callable=glo_ra5_function,
    op_args=['glo_ra5'],
    dag=dag)

#DAG Sequences
glo_r39 >> glo_r42 >> glo_r60 >> glo_r67 >> glo_r70 >> glo_r93 >> glo_r94 >> glo_ra3 >> glo_ra4 >> glo_ra5