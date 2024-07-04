from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'workflow_dag',
    default_args=default_args,
    description='A simple workflow DAG',
    schedule_interval=timedelta(hours=1),
)

ingest_branches_task = BashOperator(
    task_id='ingest_branches',
    bash_command='python ~/casestudy/ingest/ingest_branches.py',
    dag=dag,
)

ingest_sales_agents_task = BashOperator(
    task_id='ingest_sales_agents',
    bash_command='python ~/casestudy/ingest/ingest_sales_agents.py',
    dag=dag,
)

ingest_sales_transactions_task = BashOperator(
    task_id='ingest_sales_transactions',
    bash_command='python ~/casestudy/ingest/ingest_sales_transactions.py',
    dag=dag,
)

spark_etl_task = BashOperator(
    task_id='spark_etl',
    bash_command='sleep 300 && spark-submit ~casestudy/batch_etl/main_etl.py',
    dag=dag,
)

daily_dump_task = BashOperator(
    task_id='daily_dump',
    bash_command='python ~/casestudy/daily_job/daily_dump.py',
    dag=dag,
)

requirements_task = BashOperator(
    task_id='requirements',
    bash_command='python ~/casestudy/daily_job/requirements.py',
    dag=dag,
)

ingest_branches_task >> ingest_sales_agents_task >> ingest_sales_transactions_task >> spark_etl_task
