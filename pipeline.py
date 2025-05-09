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
    print("Data preprocessing complete.")
    engineer_features(adjusted_1min_data, final_news_dataset)
    print("Feature engineering complete.")
    print("Pipeline complete.")

if __name__ == "__main__":
    main()