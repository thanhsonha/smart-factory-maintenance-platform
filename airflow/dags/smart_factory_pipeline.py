from datetime import datetime 

from airflow import DAG 
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"

with DAG(
    dag_id="smart_factory_medallion_pipeline",
    start_date=datetime(2026, 6, 10),
    schedule=None, 
    catchup=False,
    tags=["smart_factory", "spark", "medallion"],
) as dag:
    
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver", 
        bash_command=f"cd {PROJECT_DIR} && source .venv/bin/activate && python src/jobs/bronze_to_silver.py",
    )
    
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"cd {PROJECT_DIR} && source .venv/bin/activate && python src/jobs/silver_to_gold.py",
    )
    
    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command="cd /opt/airflow/project && python src/jobs/data_quality_check.py",
    )
    
    bronze_to_silver >> data_quality_check >> silver_to_gold
