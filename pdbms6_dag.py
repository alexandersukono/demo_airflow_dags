# airflow related
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

# other packages
from datetime import datetime, timedelta

# import operators from the 'operators' file
from glo_waktukerja import glo_waktukerja_function
from glo_workflow import glo_workflow_function
from glo_worktype import glo_worktype_function
from hriw import hriw_function
from jobactual import jobactual_function
from jobactualcomposite_1 import jobactualcomposite_1_function
from jobactualcomposite_2 import jobactualcomposite_2_function
from jobactualcomposite import jobactualcomposite_function
from jobactualfreelance import jobactualfreelance_function
from jobactualtype_1 import jobactualtype_1_function


# import lithops function
#from jti_lithops_function import jti_lithops_function 

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("pdbms6_dag", default_args=default_args, schedule_interval=None)

glo_waktukerja = PythonOperator(
    task_id='glo_waktukerja',
    python_callable=glo_waktukerja_function,
    op_args=['glo_waktukerja'],
    dag=dag)

glo_workflow = PythonOperator(
    task_id='glo_workflow',
    python_callable=glo_workflow_function,
    op_args=['glo_glo_workflowrac'],
    dag=dag)

glo_worktype = PythonOperator(
    task_id='glo_worktype',
    python_callable=glo_worktype_function,
    op_args=['glo_worktype'],
    dag=dag)

hriw = PythonOperator(
    task_id='hriw',
    python_callable=hriw_function,
    op_args=['hriw'],
    dag=dag)

jobactual = PythonOperator(
    task_id='jobactual',
    python_callable=jobactual_function,
    op_args=['jobactual'],
    dag=dag)

jobactualcomposite_1 = PythonOperator(
    task_id='jobactualcomposite_1',
    python_callable=jobactualcomposite_1_function,
    op_args=['jobactualcomposite_1'],
    dag=dag)

jobactualcomposite_2 = PythonOperator(
    task_id='jobactualcomposite_2',
    python_callable=jobactualcomposite_2_function,
    op_args=['jobactualcomposite_2'],
    dag=dag)

jobactualcomposite = PythonOperator(
    task_id='jobactualcomposite',
    python_callable=jobactualcomposite_function,
    op_args=['jobactualcomposite'],
    dag=dag)
    
jobactualfreelance = PythonOperator(
    task_id='jobactualfreelance',
    python_callable=jobactualfreelance_function,
    op_args=['jobactualfreelance'],
    dag=dag)

jobactualtype_1 = PythonOperator(
    task_id='jobactualtype_1',
    python_callable=jobactualtype_1_function,
    op_args=['jobactualtype_1'],
    dag=dag)

#DAG Sequences
glo_waktukerja >> glo_workflow >> glo_worktype >> hriw >> jobactual >> jobactualcomposite_1 >> jobactualcomposite_2 >> jobactualcomposite >> jobactualfreelance >> jobactualtype_1