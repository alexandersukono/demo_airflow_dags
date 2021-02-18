# airflow related
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

# other packages
from datetime import datetime, timedelta

# import operators from the 'operators' file
from glo_kepegawaian import glo_kepegawaian_function
from glo_levelemployee import glo_levelemployee_function
from glo_lokasikerja import glo_lokasikerja_function
from glo_minggukerja import glo_minggukerja_function
from glo_pbcband import glo_pbcband_function
from glo_pbcdesc import glo_pbcdesc_function
from glo_pbcgoal import glo_pbcgoal_function
from glo_pbcgroup import glo_pbcgroup_function
from glo_pperiod import glo_pperiod_function
from glo_province import glo_province_function


# import lithops function
#from jti_lithops_function import jti_lithops_function 

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("pdbms1_dag", default_args=default_args, schedule_interval=None)

glo_kepegawaian = PythonOperator(
    task_id='glo_kepegawaian',
    python_callable=glo_kepegawaian_function,
    op_args=['glo_kepegawaian'],
    dag=dag)

glo_levelemployee = PythonOperator(
    task_id='glo_levelemployee',
    python_callable=glo_levelemployee_function,
    op_args=['glo_levelemployee'],
    dag=dag)

glo_lokasikerja = PythonOperator(
    task_id='glo_lokasikerja',
    python_callable=glo_lokasikerja_function,
    op_args=['glo_lokasikerja'],
    dag=dag)

glo_minggukerja = PythonOperator(
    task_id='glo_minggukerja',
    python_callable=glo_minggukerja_function,
    op_args=['glo_minggukerja'],
    dag=dag)

glo_pbcband = PythonOperator(
    task_id='glo_pbcband',
    python_callable=glo_pbcband_function,
    op_args=['glo_pbcband'],
    dag=dag)

glo_pbcdesc = PythonOperator(
    task_id='glo_pbcdesc',
    python_callable=glo_pbcdesc_function,
    op_args=['glo_pbcdesc'],
    dag=dag)

glo_pbcgoal = PythonOperator(
    task_id='glo_pbcgoal',
    python_callable=glo_pbcgoal_function,
    op_args=['glo_pbcgoal'],
    dag=dag)

glo_pbcgroup = PythonOperator(
    task_id='glo_pbcgroup',
    python_callable=glo_pbcgroup_function,
    op_args=['glo_pbcgroup'],
    dag=dag)
    
glo_pperiod = PythonOperator(
    task_id='glo_pperiod',
    python_callable=glo_pperiod_function,
    op_args=['glo_pperiod'],
    dag=dag)

glo_province = PythonOperator(
    task_id='glo_province',
    python_callable=glo_province_function,
    op_args=['glo_province'],
    dag=dag)

#DAG Sequences
glo_kepegawaian >> glo_levelemployee >> glo_lokasikerja >> glo_minggukerja >> glo_pbcband >> glo_pbcdesc >> glo_pbcgoal >> glo_pbcgroup >> glo_pperiod >> glo_province