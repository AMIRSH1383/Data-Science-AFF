from database_connection import engine
import pandas as pd
import yaml


CONFIG_PATH = "../config.yml"


def read_config(path):
    with open(path, 'r') as file:
        config = yaml.safe_load(file)

    stocks = config.get('stocks', [])


    return stocks



stocks_list = read_config(CONFIG_PATH)


def load_data(engine):
    final_news_dataset = {}
    for stock in stocks_list:
        query_condition = "WHERE financial_news.stock_name == " + stock
        tmp_df = pd.read_sql("SELECT * FROM stock_daily\n" +query_condition , con=engine)
        final_news_dataset[stock] = tmp_df


    stocks_raw_dataframe = {}
    for stock in stocks_list:
        query_condition = "WHERE stock_1m.stock_name == " + stock
        tmp_df = pd.read_sql("SELECT * FROM stock_1m\n" + query_condition, con=engine)
        stocks_raw_dataframe[stock] = tmp_df



    daily_stocks_dataframe = {}
    for stock in stocks_list:
        query_condition_1 = "WHERE stock_1d.stock_name == " + stock
        query_condition_2 = "WHERE stock_1d.aggregated == True\n"
        query_condition_3 = "WHERE stock_1d.aggregated == False\n"

        raw_df = pd.read_sql("SELECT * FROM stock_1d\n" + query_condition_3 +query_condition_1, con=engine)
        adj_df = pd.read_sql("SELECT * FROM stock_1d\n" + query_condition_2 +query_condition_1, con=engine)
        tmp_dict ={}
        tmp_dict["raw"] = raw_df
        tmp_dict["adj"] = adj_df
        daily_stocks_dataframe[stock] = tmp_dict

    return final_news_dataset, stocks_raw_dataframe, daily_stocks_dataframe