# import subprocess

# print("Starting data pipeline...\n")

# # Step 1: Load raw data from MySQL
# subprocess.run(["python", "scripts/load_data.py"], check=True)
# print("Data loaded from database.")

# # Step 2: Preprocess the data
# subprocess.run(["python", "scripts/preprocess.py"], check=True)
# print("Data preprocessing complete.")

# # Step 3: Feature engineering
# subprocess.run(["python", "scripts/feature_engineering.py"], check=True)
# print("Feature engineering complete.")

# print("\nPipeline completed successfully. Final dataset saved to /output folder.")
import pandas as pd
from scripts.database_connection import get_connection
from scripts.load_data import load_data
from scripts.preprocess import preprocess
from scripts.feature_engineering import engineer_features

def main():
    print("Starting data pipeline...")
    engine = get_connection()
    final_news_dataset, stocks_raw_dataframe, daily_stocks_dataframe = load_data(engine)
    print("Data loaded from database.")
    adjusted_1min_data = preprocess(stocks_raw_dataframe, daily_stocks_dataframe)
    print(adjusted_1min_data)        
    print("Data preprocessing complete.")
    engineer_features(adjusted_1min_data, final_news_dataset)
    # print("Feature engineering complete.")
    # print("Pipeline complete.")

if __name__ == "__main__":
    main()