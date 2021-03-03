# airflow related
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

# other packages
from datetime import datetime, timedelta

# import operators from the 'operators' file
from glo_r02 import glo_r02_function
from glo_r03 import glo_r03_function
from glo_r04 import glo_r04_function
from glo_r08 import glo_r08_function
from glo_r11 import glo_r11_function
from glo_r13 import glo_r13_function
from glo_r15 import glo_r15_function
from glo_r17 import glo_r17_function
from glo_r18 import glo_r18_function
from glo_r19 import glo_r19_function


# import lithops function
#from jti_lithops_function import jti_lithops_function 

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("pdbms2_dag", default_args=default_args, schedule_interval=None)

glo_r02 = PythonOperator(
    task_id='glo_r02',
    python_callable=glo_r02_function,
    op_args=['glo_r02'],
    dag=dag)

glo_r03 = PythonOperator(
    task_id='glo_r03',
    python_callable=glo_r03_function,
    op_args=['glo_r03'],
    dag=dag)

glo_r04 = PythonOperator(
    task_id='glo_r04',
    python_callable=glo_r04_function,
    op_args=['glo_r04'],
    dag=dag)

glo_r08 = PythonOperator(
    task_id='glo_r08',
    python_callable=glo_r08_function,
    op_args=['glo_r08'],
    dag=dag)

glo_r11 = PythonOperator(
    task_id='glo_r11',
    python_callable=glo_r11_function,
    op_args=['glo_r11'],
    dag=dag)

glo_r13 = PythonOperator(
    task_id='glo_r13',
    python_callable=glo_r13_function,
    op_args=['glo_r13'],
    dag=dag)

glo_r15 = PythonOperator(
    task_id='glo_r15',
    python_callable=glo_r15_function,
    op_args=['glo_r15'],
    dag=dag)

glo_r17 = PythonOperator(
    task_id='glo_r17',
    python_callable=glo_r17_function,
    op_args=['glo_r17'],
    dag=dag)
    
glo_r18 = PythonOperator(
    task_id='glo_r18',
    python_callable=glo_r18_function,
    op_args=['glo_r18'],
    dag=dag)

glo_r19 = PythonOperator(
    task_id='glo_r19',
    python_callable=glo_r19_function,
    op_args=['glo_r19'],
    dag=dag)

#DAG Sequences
glo_r02 >> glo_r03 >> glo_r04 >> glo_r08 >> glo_r11 >> glo_r13 >> glo_r15 >> glo_r17 >> glo_r18 >> glo_r19