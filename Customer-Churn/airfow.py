from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 14),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='churn_prediction_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    raw_data_storage = BashOperator(
        task_id='raw_data_storage',
        bash_command='python D:\Ml-Projects\Customer-Churn\src\components\raw_data_Storage.py'
    )
    
    data_ingestion = BashOperator(
        task_id='data_ingestion',
        bash_command='python D:\Ml-Projects\Customer-Churn\src\components\data_ingestion.py'
    )
    
    data_preparation = BashOperator(
        task_id='data_preparation',
        bash_command='python D:\Ml-Projects\Customer-Churn\src\components\data_preparation.py'
    )
    
    data_transformation = BashOperator(
        task_id='data_transformation',
        bash_command='python D:\Ml-Projects\Customer-Churn\src\components\data_transformation.py'
    )
    
    data_validation = BashOperator(
        task_id='data_validation',
        bash_command='python D:\Ml-Projects\Customer-Churn\src\components\data_validation.py'
    )
    
    feature_store = BashOperator(
        task_id='feature_store',
        bash_command='python D:\Ml-Projects\Customer-Churn\src\components\feature_store.py'
    )
    
    model_trainer = BashOperator(
        task_id='model_trainer',
        bash_command='python D:\Ml-Projects\Customer-Churn\src\components\model_trainer.py'
    )
    
    # Define task dependencies
    data_ingestion>> raw_data_storage >> data_preparation >> data_transformation >> data_validation >> feature_store >> model_trainer
