# dags/credit_etl_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.pipeline import ETLPipeline

# Argumentos padrão
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Criar DAG
dag = DAG(
    'credit_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL de dados de crédito',
    schedule='0 2 * * *',  # Diariamente às 2h da manhã
    catchup=False,
    tags=['etl', 'credit', 's3', 'snowflake'],
)

# Funções das tasks
def extract_task():
    pipeline = ETLPipeline()
    return pipeline.extract()

def transform_task(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract')
    
    pipeline = ETLPipeline()
    return pipeline.transform(raw_data)

def load_task(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform')
    
    pipeline = ETLPipeline()
    pipeline.load(transformed_data)

# Tasks
extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_task,
    dag=dag,
)

# Dependências
extract >> transform >> load
