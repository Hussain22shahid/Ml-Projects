from prefect import flow, task
import subprocess

@task
def run_script(script_path: str):
    """Runs a Python script using subprocess."""
    try:
        result = subprocess.run(["python", script_path], capture_output=True, text=True, check=True)
        return f"Output of {script_path}: \n{result.stdout}"
    except subprocess.CalledProcessError as e:
        return f"Error running {script_path}: \n{e.stderr}"

@flow
def churn_prediction_pipeline():
    """Main Prefect flow to run churn prediction pipeline scripts in sequence."""
    scripts = [
        r"D:\Ml-Projects\Customer-Churn\src\components\data_ingestion.py",
        r"D:\Ml-Projects\Customer-Churn\src\components\raw_data_Storage.py",
        r"D:\Ml-Projects\Customer-Churn\src\components\data_preparation.py",
        r"D:\Ml-Projects\Customer-Churn\src\components\data_transformation.py",
        r"D:\Ml-Projects\Customer-Churn\src\components\data_validation.py",
        r"D:\Ml-Projects\Customer-Churn\src\components\feature_store.py",
        r"D:\Ml-Projects\Customer-Churn\src\components\model_trainer.py",
    ]

    for script in scripts:
        output = run_script(script)
        print(output)

if __name__ == "__main__":
    churn_prediction_pipeline()
