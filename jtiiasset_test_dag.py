# airflow related
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# other packages
from datetime import datetime, timedelta

# import operators from the 'operators' file
from asset_inventory_prod import asset_inventory_prod_function
from asset_inventory_deployment_prod import asset_inventory_deployment_prod_function
from personal_prod import personal_prod_function

# import lithops function
#from jti_lithops_function import jti_lithops_function 

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 14),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),


}

dag = DAG("jtiiasset_test_dag", default_args=default_args, schedule_interval=None)

#DummyOperator DAGS here
staging_start = DummyOperator(
    task_id='Staging_Start',
    dag=dag)

staging_done = DummyOperator(
    task_id='Staging_Done',
    dag=dag)

datalake_start = DummyOperator(
    task_id='Datalake_Start',
    dag=dag)

datalake_done = DummyOperator(
    task_id='Datalake_Done',
    dag=dag)

dimension_start = DummyOperator(
    task_id='Dimension_Start',
    dag=dag)

dimension_done = DummyOperator(
    task_id='Dimension_Done',
    dag=dag)

fact_start = DummyOperator(
    task_id='Fact_Start',
    dag=dag)

fact_done = DummyOperator(
    task_id='Fact_Done',
    dag=dag)

datamart_start = DummyOperator(
    task_id='DataMart_Start',
    dag=dag)

datamart_done = DummyOperator(
    task_id='DataMart_Done',
    dag=dag)

olap_start = DummyOperator(
    task_id='OLAP_Start',
    dag=dag)

olap_done = DummyOperator(
    task_id='OLAP_Done',
    dag=dag)

#Add database staging here ...
jtiiasset = DummyOperator(
    task_id='jtiiasset',
    dag=dag)

livejtiipdbms = DummyOperator(
    task_id='livejtiipdbms',
    dag=dag)

jtiifinace = DummyOperator(
    task_id='jtiifinace',
    dag=dag)

livejtiipayroll = DummyOperator(
    task_id='livejtiipayroll',
    dag=dag)

#Add more database staging here ...


#PythonOperator extract staging tables

asset_inventory_prod = PythonOperator(
    task_id='asset_inventory_prod',
    python_callable=asset_inventory_prod_function,
    op_args=['asset_inventory'],
    dag=dag)

asset_inventory_deployment_prod = PythonOperator(
    task_id='asset_inventory_deployment_prod',
    python_callable=asset_inventory_deployment_prod_function,
    op_args=['asset_inventory_deployment'],
    dag=dag)

personal_prod = PythonOperator(
    task_id='personal_prod',
    python_callable=personal_prod_function,
    op_args=['personal'],
    dag=dag)



#DAG Sequences
staging_start >> staging_done >> datalake_start >> personal_prod >> asset_inventory_deployment_prod >> asset_inventory_prod >> datalake_done >> olap_start >> olap_done
