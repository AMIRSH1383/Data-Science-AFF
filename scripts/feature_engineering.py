
import yaml
import pandas as pd
import os

CONFIG_PATH = "../config.yml"
TRADING_END = "12:30:00"
OUTPUT_DIR = os.path.join("..", "output")

def read_config(path):
    with open(path, 'r') as file:
        config = yaml.safe_load(file)

    stocks = config.get('stocks', [])
    start_date = config.get('start_date', None)
    adjusted = config.get('adjusted', False)
    days_to_news = config.get('days_to_news', 5)
    desired_features = config.get('features', [])
    return stocks, desired_features, days_to_news

def add_label(stock_price, stock_news, days_to_news ):
    stock_price['target'] = 0 
    stock_price['days_to_news'] = 0  
    stock_price['datetime'] = pd.to_datetime(stock_price['datetime'])  

    # Example financial news dataframe
    stock_news['datetime'] = pd.to_datetime(stock_news['datetime']) 
    stock_news['time'] = stock_news['datetime'].dt.time
    stock_news['date'] = stock_news['datetime'].dt.date
    stock_news['adjusted_date'] = stock_news.apply(
    lambda row: row['date'] + pd.Timedelta(days=1) if row['time'] >= pd.to_datetime(TRADING_END).time() else row['date'],
    axis=1
    )

    news_dates = stock_news['adjusted_date'].tolist()
    #i think we need to change here in future.
    #currently it thinks there are several rows with the same date just like the 11 min data
    for news_date in news_dates:
        found = 0
        for i in range(1, 15):
            target_date = news_date - pd.Timedelta(days=i)
            if(len( stock_price.loc[stock_price['datetime'].dt.date == target_date, ['target', 'days_to_news']]) > 0):
                found +=1
                stock_price.loc[stock_price['datetime'].dt.date == target_date, ['target', 'days_to_news']] = [1, found]
            if(found >= days_to_news):
                break
    
    return  stock_price


def add_feature(the_dataframe, feature):
    return the_dataframe

def generate_output(stocks_list, final_dataset):
    for stock in stocks_list:
        final_dataset[stock].to_csv(os.path.join( OUTPUT_DIR,stock + "final_daily.csv"), index=False, encoding="utf-8")
    
    #could add a feature in .yml file in future in order to merge all of them and output as one singls csv if needed

def engineer_features(adjusted_1min_data, final_news_dataset):
    final_dataset ={}
    stocks_list, desired_features_list,  days_to_news = read_config(CONFIG_PATH)
    for stock in stocks_list:
        new_df = adjusted_1min_data[stock]
        for feature in desired_features_list:
            new_df = add_feature(new_df, feature)

        new_df = add_label(new_df, final_news_dataset[stock], days_to_news )
        final_dataset[stock] = new_df


    generate_output(stocks_list, final_dataset)