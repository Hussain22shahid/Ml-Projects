from dataclasses import dataclass
import numpy as np
import pandas as pd
from src.components.data_ingestion import DataIngestion
from src.logger import logging
from src.exception import CustomException
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import LabelEncoder, StandardScaler
from prefect import flow

@dataclass
class DataPreparationConfig:
    raw_data_path: str = os.path.join('artifacts', "data.csv")


class DataPreparation:
    def __init__(self):
        self.config = DataPreparationConfig()

    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans and prepares the dataset for ML modeling."""
        try:
            logging.info("Starting Data Preparation Module")

            # Converting float columns to int where applicable
            df = df.copy()  # Avoid modifying the original DataFrame
            for col in df.select_dtypes(include=['float64']).columns:
                df[col] = df[col].astype('int', errors='ignore')

            # Convert churn column to object type (string labels)
            df['churn'] = df['churn'].map({0: 'False', 1: 'True'})

            # Identify numerical and categorical columns
            num_cols = df.select_dtypes(include=['number']).columns.drop('customerid', errors='ignore')
            cat_cols = df.select_dtypes(include=['object']).columns.drop('churn', errors='ignore')

            logging.info("Handling Missing Values")
            # Handle missing values
            if len(cat_cols) > 0:
                mode_imputer = SimpleImputer(strategy="most_frequent")
                df[cat_cols] = mode_imputer.fit_transform(df[cat_cols])

            if len(num_cols) > 0:
                median_imputer = SimpleImputer(strategy="median")
                df[num_cols] = median_imputer.fit_transform(df[num_cols])

            logging.info("Removing duplicate values")
            df.drop_duplicates(inplace=True)
            df.reset_index(drop=True, inplace=True)

            logging.info("Encoding Categorical Variables")
            le = LabelEncoder()
            for col in cat_cols:
                df[col] = le.fit_transform(df[col].astype(str))  # Ensure string type before encoding

            logging.info("Scaling Numerical Features")
            scaler = StandardScaler()
            df[num_cols] = scaler.fit_transform(df[num_cols])

            logging.info("Generating Visualizations")

            # Plot numerical feature distributions
            df[num_cols].hist(figsize=(10, 6), bins=20)
            plt.suptitle("Feature Distributions")
            plt.tight_layout()
            plt.show()
            plt.savefig("img/numerical.png")
            plt.close()


            # Boxplots to detect outliers
            plt.figure(figsize=(10, 6))
            sns.boxplot(data=df[num_cols])
            plt.xticks(rotation=90)
            plt.title("Outlier Detection")
            plt.show()
            plt.savefig("img/Boxplots.png")
            plt.close()

            # Churn Distribution Visualization
            plt.figure(figsize=(5, 4))
            sns.countplot(x="churn", data=df)
            plt.title("Churn Distribution")
            plt.show()
            plt.savefig("img/Distribution.png")
            plt.close()

            logging.info("Data Preparation Completed")
            return df

        except Exception as e:
            logging.error(f"Error in Data Preparation: {e}")
            raise CustomException(e, sys)

    def call_prep(self) -> pd.DataFrame:
        """Handles data ingestion and preparation in sequence."""
        ingestion_obj = DataIngestion()
        raw_data = ingestion_obj.call()  # Fetch data

        if raw_data is not None:
            logging.info("Data ingestion successful! Proceeding with preparation.")
            processed_data = self.prepare_data(raw_data)
            return processed_data
        else:
            logging.error("Data ingestion failed.")
            return None


if __name__ == "__main__":
    obj = DataPreparation()
    cleaned_data = obj.call_prep()

    if cleaned_data is not None:
        logging.info("Data Preparation Successful! Here are the first few rows:")
        logging.info(cleaned_data.head())  # Display sample data
        logging.info("Shape:", cleaned_data.shape)
    else:
        logging.info("Data preparation failed.")
