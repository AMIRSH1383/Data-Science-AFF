import pandas as pd
from sqlalchemy import create_engine
import os
import numpy as np

# MySQL credentials
USERNAME = 'root'
PASSWORD = 'Amirarsalan83'
host = 'localhost'
port = 3306
database = 'stock_market' 

BASE_DIR = os.path.join("..", "..", "raw_data")
BASE_DIR_1M = os.path.join(BASE_DIR, "1min_agg")
BASE_DIR_NEWS = os.path.join(BASE_DIR, "financial_news")
BASE_DIR_1D_RAW = os.path.join(BASE_DIR, "daily", "raw")
BASE_DIR_1D_ADJ = os.path.join(BASE_DIR, "daily", "adjusted")
# Connect to MySQL
engine = create_engine(f"mysql+pymysql://{USERNAME}:{PASSWORD}@{host}:{port}/{database}")

list_of_stocks = ["fars", "foolad", "khodro", "ranfor", "toreil", "zagros"]


# Load financial news
for stock_name in list_of_stocks:
    news_path = os.path.join(BASE_DIR_NEWS, f"{stock_name}.csv")
    if os.path.exists(news_path):
        df_financial_news = pd.read_csv(news_path)
        # Rename the mismatched columns
        df_financial_news.rename(columns={
            "datetime" : "date_time",
            "title": "news_title",
            "code": "news_code",
        }, inplace=True)
        df_financial_news.rename({"datetime" : "date_time"}, inplace=True)
        df_financial_news["stock_name"] = stock_name
        # print(df_financial_news.head())
        df_financial_news.to_sql("financial_news", con=engine, if_exists="append", index=False)
        print(f"Loaded financial news for {stock_name}")
    else:
        print(f"File not found: {news_path}")

# Load data 1m
for  stock_name in list_of_stocks:
    # base_dir = r"G:\University\Term 6\Data science\Project\2\processed_datasets\historical_data\agg_data"
    path_1m = os.path.join(BASE_DIR_1M, f"{stock_name}.csv")
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
            "close": "close_price",
            "volume" : "volume",
            "avg_price": "avg_price",
            "trade_number":"trade_number"
        }, inplace=True)

        df_1m["stock_name"] = stock_name
        df_1m.to_sql("stock_1m", con=engine, if_exists="append", index=False)
        print(f"Loaded data 1m for {stock_name}")
    else:
        print(f"File not found: {path_1m}")



# Load data daily raw
for stock_name in list_of_stocks:
    # base_dir = r"G:\University\Term 6\Data science\Project\2\final_datasets"
    path_daily = os.path.join(BASE_DIR_1D_RAW, f"{stock_name}.csv")
    if os.path.exists(path_daily):
        df_daily = pd.read_csv(path_daily)
        if "Unnamed: 0" in df_daily.columns:
            df_daily.drop(columns=["Unnamed: 0"], inplace=True)
        if "<TICKER>" in df_daily.columns:
            df_daily.drop(columns=["<TICKER>"], inplace=True)
        if "<OPENINT>" in df_daily.columns:
            df_daily.drop(columns=["<OPENINT>"], inplace=True)
            df_daily.drop(columns=["<OPENINT>.1"], inplace=True)

        # Rename the mismatched columns
        df_daily.rename(columns={
            "<DTYYYYMMDD>" : "date_time",
            "<OPEN>": "open_price",
            "<HIGH>": "highest_price",
            "<LOW>": "lowest_price",
            "<CLOSE>": "close_price",
            "<VOL>": "volume"
        }, inplace=True)

        # Fix inf/-inf and NaNs
        df_daily.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_daily.dropna(inplace=True)

        df_daily["stock_name"] = stock_name
        df_daily["adjusted"] = False
        df_daily.to_sql("stock_daily", con=engine, if_exists="append", index=False)
        print(f"Loaded data daily for {stock_name}")
    else:
        print(f"File not found: {path_daily}")



# Load data daily adjusted
for stock_name in list_of_stocks:
    # base_dir = r"G:\University\Term 6\Data science\Project\2\final_datasets"
    path_daily = os.path.join(BASE_DIR_1D_ADJ, f"{stock_name}.csv")
    if os.path.exists(path_daily):
        df_daily = pd.read_csv(path_daily)
        if "Unnamed: 0" in df_daily.columns:
            df_daily.drop(columns=["Unnamed: 0"], inplace=True)
        if "<TICKER>" in df_daily.columns:
            df_daily.drop(columns=["<TICKER>"], inplace=True)
        if "<OPENINT>" in df_daily.columns:
            df_daily.drop(columns=["<OPENINT>"], inplace=True)
            df_daily.drop(columns=["<OPENINT>.1"], inplace=True)

        # Rename the mismatched columns
        df_daily.rename(columns={
            "<DTYYYYMMDD>" : "date_time",
            "<OPEN>": "open_price",
            "<HIGH>": "highest_price",
            "<LOW>": "lowest_price",
            "<CLOSE>": "close_price",
            "<VOL>": "volume"
        }, inplace=True)

        # Fix inf/-inf and NaNs
        df_daily.replace([np.inf, -np.inf], np.nan, inplace=True)
        df_daily.dropna(inplace=True)

        df_daily["stock_name"] = stock_name
        df_daily["adjusted"] = True
        df_daily.to_sql("stock_daily", con=engine, if_exists="append", index=False)
        print(f"Loaded data daily for {stock_name}")
    else:
        print(f"File not found: {path_daily}")
print("All data inserted successfully.")
