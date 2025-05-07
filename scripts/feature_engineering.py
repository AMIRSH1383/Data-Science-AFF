
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
    df['price_change'] = (df['close_price'] / df['close_price'].shift(1))-1
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








def basic_ohlc_features(group):
    open_price = group['open_price'].iloc[0]
    high_price = group['highest_price'].max()
    low_price = group['lowest_price'].min()
    close_price = group['close_price'].iloc[-1]
    volume = group['volume'].sum()
    trade_value = (group['trade_value']).sum()
    avg_price = (group['avg_price'] * group['volume']).sum() / group['volume'].sum() if volume > 0 else 0

    return pd.Series({
        'open_price': open_price,
        'high_price': high_price,
        'low_price': low_price,
        'close_price': close_price,
        'avg_price': avg_price,
        'volume': volume,
        'trade_value': trade_value
    })


def wick_and_body_ratios_features(group):
    o = group['open_price'].iloc[0]
    c = group['close_price'].iloc[-1]
    h = group['highest_price'].max()
    l = group['lowest_price'].min()
    ap = ((group['avg_price'] * group['volume']).sum() / group['volume'].sum()) if group['volume'].sum() > 0 else 0
    denom = h - l if h != l else 1
    body =  (max(o, c) - min(o, c)) / denom
    upper_wick = (h - max(o, c)) / denom
    lower_wick = (min(o, c) - l) / denom
    avg_pos = (ap - l) / denom
    return pd.Series({
        'body_divided_by_high_minus_low': body,
        'upper_wick_to_high_minus_low': upper_wick,
        'lower_wick_to_high_minus_low': lower_wick,
        'avg_price_position_with_respect_the_high_and_low': avg_pos
    })


def rolling_extrema(group, extreme_period = 10):
    price_change = group['price_change']
    trade_value = group['trade_value']
    max_n_pos_pc = price_change.rolling(extreme_period).sum().max()
    min_n_neg_pc = price_change.rolling(extreme_period).sum().min()
    max_n_tv = trade_value.rolling(extreme_period).sum().max()
    return pd.Series({
        f'max_{extreme_period}_min_positive_price_change': max_n_pos_pc,
        f'max_{extreme_period}_min_negative_price_change': min_n_neg_pc,
        f'max_{extreme_period}_mins_trade_value': max_n_tv
    })


def count_zero_and_significant(group, sign_thresh= 0.01):
    zero_vol = (group['volume'] == 0).sum()
    sig = (group['price_change'].abs() > sign_thresh).sum()
    return pd.Series({
        'zero_volume_minutes_count': zero_vol,
        'significant_minutes_count': sig
    })


def count_rsi_thresholds(group, upper = 80, lower = 20):

    above = (group['RSI'] > upper).sum()
    below = (group['RSI'] < lower).sum()
    return pd.Series({
        'RSI_above_{}'.format(upper): above,
        'RSI_under_{}'.format(lower): below
    })


def count_bb_breaks(group):
    above = (group['highest_price'] > group['BB_higher']).sum()
    below = (group['lowest_price'] < group['BB_lower']).sum()
    return pd.Series({
        'price_higher_than_BB_higher_count': above,
        'price_lower_than_BB_lower_count': below
    })

def count_ichimoku_crosses(group):
    kijun = group['kijunsen']
    tenkan = group['tenkensen']
    senko_a = group['senko_a']
    senko_b = group['senko_b']
    close = group['close_price']

    # kijun-tenkan crosses
    sign_diff = (kijun - tenkan).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    kijun_tenken_cross = (sign_diff.diff().abs() > 0).sum()

    # price in cloud
    lower = pd.concat([senko_a, senko_b], axis=1).min(axis=1)
    upper = pd.concat([senko_a, senko_b], axis=1).max(axis=1)
    in_cloud = ((close >= lower) & (close <= upper)).sum()

    # price between kijun and tenkan
    lower_kt = pd.concat([kijun, tenkan], axis=1).min(axis=1)
    upper_kt = pd.concat([kijun, tenkan], axis=1).max(axis=1)
    between_kt = ((close > lower_kt) & (close < upper_kt)).sum()

    # cloud boundary crosses (using senko_a)
    cloud_sign = (close - senko_a).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    cloud_cross = (cloud_sign.diff().abs() > 0).sum()

    return pd.Series({
        'kijun_tenken_cross_count': kijun_tenken_cross,
        'price_in_cloud_minutes_count': in_cloud,
        'price_between_kijun_tenkensen_count': between_kt,
        'cloud_cross_count': cloud_cross
    })


def count_macd_signal_cross(group):
    macd = group['MACD']
    signal = group['signal']
    macd_sign = (macd - signal).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    cross = (macd_sign.diff().abs() > 0).sum()
    return pd.Series({'macd_cross_signal_count': cross})


def aggregate_daily(enhanced_1min_df):
    df = enhanced_1min_df.copy()
    df['date_time'] = pd.to_datetime(df['date_time'])
    df['date'] = df['date_time'].dt.date

    aggregators = [
        basic_ohlc_features,
        wick_and_body_ratios_features,
        lambda g: rolling_extrema(g, extreme_period=10),
        count_zero_and_significant,
        count_rsi_thresholds,
        count_bb_breaks,
        count_ichimoku_crosses,
        count_macd_signal_cross
    ]

    records = []
    for date, group in df.groupby('date'):
        features = {}
        for func in aggregators:
            features.update(func(group).to_dict())
        features['date'] = date
        records.append(features)
    return pd.DataFrame(records)


def engineer_features(adjusted_1min_data, final_news_dataset):
    final_dataset ={}
    stocks_list, desired_features_list,  days_to_news = read_config(CONFIG_PATH)

    for stock in stocks_list:
        new_df = adjusted_1min_data[stock]
        enhanced_1min_dataframe = prepare_enhanced_1min_dataframe(new_df)
        base_daily_dataframe = aggregate_daily(enhanced_1min_dataframe)
        # new_df = add_label(new_df, final_news_dataset[stock], days_to_news )
        # enhanced_1min_dataframe.to_csv("./output/test.csv")
        final_dataset[stock] = base_daily_dataframe
        # final_dataset[stock] = enhanced_1min_dataframe


    generate_output(stocks_list, final_dataset)