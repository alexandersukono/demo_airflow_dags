# airflow related
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

# other packages
from datetime import datetime, timedelta

# import operators from the 'operators' file
from glo_ra9 import glo_ra9_function
from glo_rac import glo_rac_function
from glo_rax import glo_rax_function
from glo_ray import glo_ray_function
from glo_rbw import glo_rbw_function
from glo_rlokasikerja import glo_rlokasikerja_function
from glo_statusemp import glo_statusemp_function
from glo_taxjamsos import glo_taxjamsos_function
from glo_ump import glo_ump_function
from glo_unitkecil import glo_unitkecil_function


# import lithops function
#from jti_lithops_function import jti_lithops_function 

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("pdbms5_dag", default_args=default_args, schedule_interval=None)

glo_ra9 = PythonOperator(
    task_id='glo_ra9',
    python_callable=glo_ra9_function,
    op_args=['glo_ra9'],
    dag=dag)

glo_rac = PythonOperator(
    task_id='glo_rac',
    python_callable=glo_rac_function,
    op_args=['glo_rac'],
    dag=dag)

glo_rax = PythonOperator(
    task_id='glo_rax',
    python_callable=glo_rax_function,
    op_args=['glo_rax'],
    dag=dag)

glo_ray = PythonOperator(
    task_id='glo_ray',
    python_callable=glo_ray_function,
    op_args=['glo_ray'],
    dag=dag)

glo_rbw = PythonOperator(
    task_id='glo_rbw',
    python_callable=glo_rbw_function,
    op_args=['glo_rbw'],
    dag=dag)

glo_rlokasikerja = PythonOperator(
    task_id='glo_rlokasikerja',
    python_callable=glo_rlokasikerja_function,
    op_args=['glo_rlokasikerja'],
    dag=dag)

glo_statusemp = PythonOperator(
    task_id='glo_statusemp',
    python_callable=glo_statusemp_function,
    op_args=['glo_statusemp'],
    dag=dag)

glo_taxjamsos = PythonOperator(
    task_id='glo_taxjamsos',
    python_callable=glo_taxjamsos_function,
    op_args=['glo_taxjamsos'],
    dag=dag)
    
glo_ump = PythonOperator(
    task_id='gglo_umplo_ra4',
    python_callable=glo_ump_function,
    op_args=['glo_ump'],
    dag=dag)

glo_unitkecil = PythonOperator(
    task_id='glo_unitkecil',
    python_callable=glo_unitkecil_function,
    op_args=['glo_unitkecil'],
    dag=dag)

#DAG Sequences
glo_ra9 >> glo_rac >> glo_rax >> glo_ray >> glo_rbw >> glo_rlokasikerja >> glo_statusemp >> glo_taxjamsos >> glo_ump >> glo_unitkecil