from prefect import flow, task
import subprocess
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@task(retries=3, retry_delay_seconds=5)
def run_script(script_path: str, flow_name: str):
    """Runs a Python script using subprocess and logs the output with flow name."""
    try:
        result = subprocess.run(["python", script_path], capture_output=True, text=True, check=True)
        logging.info(f"Successfully ran {flow_name}")
        return f"Output of {flow_name}:\n{result.stdout}"
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in {flow_name}:\n{e.stderr}")
        raise e  # Ensures Prefect detects failure

@flow
def churn_prediction_pipeline():
    """Main Prefect flow to run churn prediction pipeline scripts in sequence."""
    
    script_flows = {
        r"D:\Ml-Projects\Customer-Churn\src\components\data_ingestion.py": "Data Ingestion",
        r"D:\Ml-Projects\Customer-Churn\src\components\raw_data_Storage.py": "Raw Data Storage",
        r"D:\Ml-Projects\Customer-Churn\src\components\data_preparation.py": "Data Preparation",
        r"D:\Ml-Projects\Customer-Churn\src\components\data_transformation.py": "Data Transformation",
        r"D:\Ml-Projects\Customer-Churn\src\components\data_validation.py": "Data Validation",
        r"D:\Ml-Projects\Customer-Churn\src\components\feature_store.py": "Feature Store",
        r"D:\Ml-Projects\Customer-Churn\src\components\model_trainer.py": "Model Trainer",
    }

    results = [run_script(script, flow_name) for script, flow_name in script_flows.items()]
    
    for output in results:
        logging.info(output)

if __name__ == "__main__":
    churn_prediction_pipeline()
