"""
Minimal Test DAG for debugging Airflow 3.0.0 execution issues
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Default arguments
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'test_simple_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['test'],
)

def simple_task():
    """A very simple task that just prints and returns"""
    print("Hello from simple task!")
    print("Task is working correctly!")
    return "success"

# Create task
test_task = PythonOperator(
    task_id='simple_test_task',
    python_callable=simple_task,
    dag=dag,
) 