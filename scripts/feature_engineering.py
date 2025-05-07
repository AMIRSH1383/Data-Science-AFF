
import yaml
import pandas as pd
import os
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands
from ta.trend import IchimokuIndicator, MACD


CONFIG_PATH = "config.yml"
TRADING_END = "12:30:00"
OUTPUT_DIR = "output"

def read_config(path):
    with open(path, 'r') as file:
        config = yaml.safe_load(file)

    stocks = config.get('stocks', [])
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


def generate_output(stocks_list, final_dataset):
    for stock in stocks_list:
        final_dataset[stock].to_csv(os.path.join( OUTPUT_DIR,stock + "final_daily.csv"), index=False, encoding="utf-8")
    
    #could add a feature in .yml file in future in order to merge all of them and output as one singls csv if needed

def add_trade_value(df):
    df['trade_value'] = df['volume'] * df['avg_price']
    return df

def add_price_change(df):
    df['price_change'] = df['close_price'] / df['close_price'].shift(1)
    return df

def add_rsi(df, window = 14) :
    rsi = RSIIndicator(close=df['close_price'], window=window)
    df['RSI'] = rsi.rsi()
    return df

def add_bollinger_bands(df, window = 20, window_dev = 2):

    bb = BollingerBands(close=df['close_price'], window=window, window_dev=window_dev)
    df['BB_higher'] = bb.bollinger_hband()
    df['BB_lower'] = bb.bollinger_lband()
    return df

def add_ichimoku(df, window1= 9, window2 = 26, window3 = 52):

    ichi = IchimokuIndicator(high=df['highest_price'],
                              low=df['lowest_price'],
                              window1=window1,
                              window2=window2,
                              window3=window3)
    
    df['tenkensen'] = ichi.ichimoku_conversion_line()
    df['kijunsen'] = ichi.ichimoku_base_line()
    df['senko_a'] = ichi.ichimoku_a()
    df['senko_b'] = ichi.ichimoku_b()
    return df

def add_macd(df, window_slow = 26, window_fast = 12, window_sign= 9):

    macd = MACD(close=df['close_price'],
                window_slow=window_slow,
                window_fast=window_fast,
                window_sign=window_sign)
    df['MACD'] = macd.macd()
    df['signal'] = macd.macd_signal()
    return df


def prepare_enhanced_1min_dataframe(base_1min_data):
    df = base_1min_data.sort_values('date_time')
    funcs = [
        add_trade_value,
        add_price_change,
        add_rsi,
        add_bollinger_bands,
        add_ichimoku,
        add_macd
    ]
    for func in funcs:
        df = func(df)
    return df

def engineer_features(adjusted_1min_data, final_news_dataset):
    final_dataset ={}
    stocks_list, desired_features_list,  days_to_news = read_config(CONFIG_PATH)

    for stock in stocks_list:
        new_df = adjusted_1min_data[stock]
        enhanced_1min_dataframe = prepare_enhanced_1min_dataframe(new_df)

        # new_df = add_label(new_df, final_news_dataset[stock], days_to_news )
        # enhanced_1min_dataframe.to_csv("./output/test.csv")
        final_dataset[stock] = enhanced_1min_dataframe
        # final_dataset[stock] = enhanced_1min_dataframe


    generate_output(stocks_list, final_dataset)