import pandas as pd
from sqlalchemy import create_engine
import os
import numpy as np

# MySQL credentials
username = 'Amirarsalan'
# password = 'your_password'
host = 'localhost'
port = 3306
database = 'stock_market' 

# Connect to MySQL
engine = create_engine(f"mysql+pymysql://{username}@{host}:{port}/{database}")

list_of_stocks = [
    ("fars", "Fars"),
    ("foolad", "Foolad"),
    ("khodro", "Khodro"),
    ("ranfor", "Ranfor"),
    ("toreil", "Toreil"),
    ("zagros", "Zagros")
    ]

# Load financial news
for file_name_prefix, stock_name in list_of_stocks:
    base_dir = r"G:\University\Term 6\Data science\Project\2\processed_datasets\financial_news"
    news_path = os.path.join(base_dir, f"{file_name_prefix}.csv")
    if os.path.exists(news_path):
        df_financial_news = pd.read_csv(news_path)

        # Rename the mismatched columns
        df_financial_news.rename(columns={
            "title": "news_title",
            "code": "news_code",
            "datetime" : "date_time"
        }, inplace=True)

        df_financial_news["stock_name"] = stock_name
        df_financial_news.to_sql("financial_news", con=engine, if_exists="append", index=False)
        print(f"Loaded financial news for {stock_name}")
    else:
        print(f"File not found: {news_path}")

# Load data 1m
for file_name_prefix, stock_name in list_of_stocks:
    base_dir = r"G:\University\Term 6\Data science\Project\2\processed_datasets\historical_data\agg_data"
    path_1m = os.path.join(base_dir, f"{file_name_prefix}_agg.csv")
    if os.path.exists(path_1m):
        df_1m = pd.read_csv(path_1m)
        if "Unnamed: 0" in df_1m.columns:
            df_1m.drop(columns=["Unnamed: 0"], inplace=True)
        # Rename the mismatched columns
        df_1m.rename(columns={
            "datetime" : "date_time",
            "open": "open_price",
            "high": "highest_price",
            "low": "lowest_price",
            "close": "close_price"
        }, inplace=True)

        df_1m["stock_name"] = stock_name
        df_1m.to_sql("stock_1m", con=engine, if_exists="append", index=False)
        print(f"Loaded data 1m for {stock_name}")
    else:
        print(f"File not found: {path_1m}")

# Load data daily
for file_name_prefix, stock_name in list_of_stocks:
    base_dir = r"G:\University\Term 6\Data science\Project\2\final_datasets"
    path_daily = os.path.join(base_dir, f"{file_name_prefix}_final_daily.csv")
    if os.path.exists(path_daily):
        df_daily = pd.read_csv(path_daily)
        if "Unnamed: 0" in df_daily.columns:
            df_daily.drop(columns=["Unnamed: 0"], inplace=True)
        # Rename the mismatched columns
        df_daily.rename(columns={
            "date" : "transaction_date",
            "open": "open_price",
            "high": "highest_price",
            "low": "lowest_price",
            "close": "close_price",
            "30d_avg_trade_value": "trade_value_30d_avg"
        }, inplace=True)

        # Fix inf/-inf and NaNs
        df_daily.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_daily.dropna(inplace=True)

        df_daily["stock_name"] = stock_name
        df_daily.to_sql("stock_daily", con=engine, if_exists="append", index=False)
        print(f"Loaded data daily for {stock_name}")
    else:
        print(f"File not found: {path_daily}")

print("All data inserted successfully.")
