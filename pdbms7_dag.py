# airflow related
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

# other packages
from datetime import datetime, timedelta

# import operators from the 'operators' file
from jobactualtype import jobactualtype_function
from jobhist import jobhist_function
from jobsalary import jobsalary_function
from jobsalary2 import jobsalary2_function
from jobsalaryfreelance import jobsalaryfreelance_function
from kalender import kalender_function
from kalendernote import kalendernote_function
from kalenderpro import kalenderpro_function
from kalenderwd import kalenderwd_function
from kalenderwt import kalenderwt_function
from kalstandby import kalstandby_function
from keluarga import keluarga_function
from keluargafreelance import keluargafreelance_function
from kursus import kursus_function



# import lithops function
#from jti_lithops_function import jti_lithops_function 

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("pdbms7_dag", default_args=default_args, schedule_interval=None)

jobactualtype = PythonOperator(
    task_id='jobactualtype',
    python_callable=jobactualtype_function,
    op_args=['jobactualtype'],
    dag=dag)

jobhist = PythonOperator(
    task_id='jobhist',
    python_callable=jobhist_function,
    op_args=['jobhist'],
    dag=dag)

jobsalary = PythonOperator(
    task_id='jobsalary',
    python_callable=jobsalary_function,
    op_args=['jobsalary'],
    dag=dag)

jobsalary2 = PythonOperator(
    task_id='jobsalary2',
    python_callable=jobsalary2_function,
    op_args=['jobsalary2'],
    dag=dag)

jobsalaryfreelance = PythonOperator(
    task_id='jobsalaryfreelance',
    python_callable=jobsalaryfreelance_function,
    op_args=['jobsalaryfreelance'],
    dag=dag)

kalender = PythonOperator(
    task_id='kalender',
    python_callable=kalender_function,
    op_args=['kalender'],
    dag=dag)

kalendernote = PythonOperator(
    task_id='kalendernote',
    python_callable=kalendernote_function,
    op_args=['kalendernote'],
    dag=dag)

kalenderpro = PythonOperator(
    task_id='kalenderpro',
    python_callable=kalenderpro_function,
    op_args=['kalenderpro'],
    dag=dag)
    
kalenderwd = PythonOperator(
    task_id='kalenderwd',
    python_callable=kalenderwd_function,
    op_args=['kalenderwd'],
    dag=dag)

kalenderwt = PythonOperator(
    task_id='kalenderwt',
    python_callable=kalenderwt_function,
    op_args=['kalenderwt'],
    dag=dag)

kalstandby = PythonOperator(
    task_id='kalstandby',
    python_callable=kalstandby_function,
    op_args=['kalstandby'],
    dag=dag)

keluarga = PythonOperator(
    task_id='keluarga',
    python_callable=keluarga_function,
    op_args=['keluarga'],
    dag=dag)

keluargafreelance = PythonOperator(
    task_id='keluargafreelance',
    python_callable=keluargafreelance_function,
    op_args=['keluargafreelance'],
    dag=dag)

kursus = PythonOperator(
    task_id='kursus',
    python_callable=kursus_function,
    op_args=['kursus'],
    dag=dag)

#DAG Sequences
jobactualtype >> jobhist >> jobsalary >> jobsalary2 >> jobsalaryfreelance >> kalender >> kalendernote >> kalenderpro >> kalenderwd >> kalenderwt >> kalstandby >> keluarga >> keluargafreelance >> kursus