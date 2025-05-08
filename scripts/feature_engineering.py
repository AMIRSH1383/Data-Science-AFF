
import yaml
import pandas as pd
import os
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands
from ta.trend import IchimokuIndicator, MACD


CONFIG_PATH = "config.yml"
TRADING_END = "12:30:00"
OUTPUT_DIR = "output"

DEFAULT_RSI_UPPER_TRESH = 80
DEFAULT_RSI_LOWER_TRESH = 20
DEFAULT_RSI_PERIOD = 14
DEFAULT_INTERVAL_DURATION = 10
DEFAULT_VOL_VAL_AVG_PERIOD = 20
DEFAULT_BB_PERIOD = 20
DEFAULT_SIGNIFICANT_MIN_TRESH = 0.01

def get_preferred_params(config):
    preferred_parameters ={}
    vol_val_average_period = config.get('vol_val_average_period', DEFAULT_VOL_VAL_AVG_PERIOD)
    rsi_period = config.get('rsi_period', DEFAULT_RSI_PERIOD)
    interval_duration = config.get('interval_duration', [DEFAULT_INTERVAL_DURATION])
    rsi_higher_tresh = config.get('rsi_higher_tresh', DEFAULT_RSI_UPPER_TRESH)
    rsi_lower_tresh = config.get('rsi_lower_tresh', DEFAULT_RSI_LOWER_TRESH)
    bb_n = config.get('bb_n', DEFAULT_BB_PERIOD)
    significant_min_tresh = config.get('significant_min_tresh', DEFAULT_SIGNIFICANT_MIN_TRESH)

    preferred_parameters['vol_val_average_period'] = vol_val_average_period
    preferred_parameters['rsi_period'] = rsi_period
    preferred_parameters['interval_duration'] = interval_duration
    preferred_parameters['rsi_higher_tresh'] = rsi_higher_tresh
    preferred_parameters['rsi_lower_tresh'] = rsi_lower_tresh
    preferred_parameters['bb_n'] = bb_n
    preferred_parameters['significant_min_tresh'] = significant_min_tresh

    return preferred_parameters

def read_config(path):
    with open(path, 'r') as file:
        config = yaml.safe_load(file)

    stocks = config.get('stocks', [])
    days_to_news = config.get('days_to_news', 5)
    desired_features = config.get('features', [])

    preferred_parameters = get_preferred_params(config)

    return stocks, desired_features, days_to_news, preferred_parameters

def add_label(stock_price, stock_news, days_to_news ):
    stock_price_labled = stock_price.copy()
    stock_price_labled.loc[:, 'target'] = 0
    stock_price_labled.loc[:, 'days_to_news'] = 0
    stock_price_labled.loc[:, 'datetime'] = pd.to_datetime(stock_price_labled['date']) 

    # Example financial news dataframe
    stock_news['datetime'] = pd.to_datetime(stock_news['date_time']) 
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
            if(len( stock_price_labled.loc[stock_price_labled['datetime'].dt.date == target_date, ['target', 'days_to_news']]) > 0):
                found +=1
                stock_price_labled.loc[stock_price_labled['datetime'].dt.date == target_date, ['target', 'days_to_news']] = [1, found]
            if(found >= days_to_news):
                break
    stock_price_labled.drop(columns=['datetime'], inplace=True)
    return  stock_price_labled

def generate_output(stocks_list, final_dataset):

    dfs = []
    for stock in stocks_list:
        final_dataset[stock].to_csv(os.path.join(OUTPUT_DIR, stock + "final_daily.csv"), index=False, encoding="utf-8")
        df = final_dataset[stock].copy()
        df['stock_name'] = stock
        dfs.append(df)
    
    all_data = pd.concat(dfs, axis=0, ignore_index=True)
    all_data.to_csv(os.path.join(OUTPUT_DIR, "final_dataset.csv"), index=False, encoding='utf-8')
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
    df['senko_a'] = ichi.ichimoku_a().shift(window2-1)
    df['senko_b'] = ichi.ichimoku_b().shift(window2-1)
    return df

def add_macd(df, window_slow = 26, window_fast = 12, window_sign= 9):

    macd = MACD(close=df['close_price'],
                window_slow=window_slow,
                window_fast=window_fast,
                window_sign=window_sign)
    df['MACD'] = macd.macd()
    df['signal'] = macd.macd_signal()
    return df


def prepare_enhanced_1min_dataframe(base_1min_data, preferred_parameters):
    df = base_1min_data.sort_values('date_time')
    
    funcs = [
        add_trade_value,
        add_price_change,
        lambda g: add_rsi(g, window=preferred_parameters['rsi_period']),
        lambda g: add_bollinger_bands(g, window=preferred_parameters['bb_n']),
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
        'highest_price': high_price,
        'lowest_price': low_price,
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
        f'max_{extreme_period}_min_trade_value': max_n_tv
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
        'RSI_above_treshold': above,
        'RSI_under_treshold': below
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

def intraday_volatility(group):
    returns = group['price_change'] - 1
    vol = returns.std()
    return pd.Series({'intraday_volatility': vol})

def aggregate_daily(enhanced_1min_df, preferred_parameters):
    df = enhanced_1min_df.copy()
    df['date_time'] = pd.to_datetime(df['date_time'])
    df['date'] = df['date_time'].dt.date


    aggregators = [
        basic_ohlc_features,
        wick_and_body_ratios_features,
        count_bb_breaks,
        intraday_volatility,
        count_ichimoku_crosses,
        count_macd_signal_cross,
        count_zero_and_significant,
        lambda g: count_zero_and_significant(g, sign_thresh= preferred_parameters['significant_min_tresh']),
        lambda g: count_rsi_thresholds(g, upper=preferred_parameters['rsi_higher_tresh'], lower = preferred_parameters['rsi_lower_tresh'])
        
    ]
    for duration in preferred_parameters['interval_duration']:
        aggregators.append(lambda g, d=duration: rolling_extrema(g, extreme_period=d))

    records = []
    for date, group in df.groupby('date'):
        features = {}
        for func in aggregators:
            features.update(func(group).to_dict())
        features['date'] = date
        records.append(features)
    pd.DataFrame(records).to_csv("output/test_extreme.csv")
    return pd.DataFrame(records)







def add_daily_returns(df):
    df = df.copy()
    df['prev_close'] = df['close_price'].shift(1)
    df['open_return'] = df['open_price'] / df['prev_close']
    df['high_return'] = df['highest_price'] / df['prev_close']
    df['low_return'] = df['lowest_price'] / df['prev_close']
    df['close_return'] = df['close_price'] / df['prev_close']
    df['avg_price_return'] = df['avg_price'] / df['prev_close']
    df.drop(columns=['prev_close'], inplace=True)
    return df

def add_rolling_averages(df, window = 22):
    df = df.copy()
    df[f'avg_{window}_day_volume'] = df['volume'].rolling(window).mean()
    df[f'avg_{window}_day_trade_value'] = df['trade_value'].rolling(window).mean()
    return df


def add_high_low_div_cloud(df):
    df = df.copy()
    cloud_range = (df['senko_b'] - df['senko_a']).abs()
    denom = cloud_range.replace(0, 1)
    df['high_minus_low_divided_by_cloud_range'] = (df['highest_price'] - df['lowest_price']) / denom
    return df

def add_volume_tradevalue_ratios(df, window, intervals):
    df = df.copy()
    df['vol_div_avg_vol'] = df['volume'] / df.get(f'avg_{window}_day_volume')
    df['trade_value_div_avg_trade_value'] = df['trade_value'] / df.get(f'avg_{window}_day_trade_value')
    for interval in intervals:
        df[f'max_{interval}_min_trade_value_div_avg_trade_value'] = df.get(f'max_{interval}_min_trade_value') / df[f'avg_{window}_day_trade_value']
    
    return df


def build_sophisticated_daily(df, preferred_parameters):
    df = df.copy()
    
    funcs = [
        add_daily_returns,
        add_ichimoku,
        add_high_low_div_cloud,
        lambda g: add_rsi(g, window=preferred_parameters['rsi_period']),
        lambda g: add_rolling_averages(g, window=preferred_parameters['vol_val_average_period']),
        lambda g: add_volume_tradevalue_ratios(g, window=preferred_parameters['vol_val_average_period'], intervals = preferred_parameters['interval_duration'])
    ]
    for func in funcs:
        df = func(df)
    return df

def select_features(all_features_daily_dataframe, desired_features_list, preferred_parameters):
    for feature in ['max_negative_price_change', 'max_positive_price_change', 'max_trade_value_div_avg_trade_value']:
        if(feature in desired_features_list):
            desired_features_list.remove(feature)
            for window in preferred_parameters['interval_duration']:
                new_feature =  feature[0:4] + f"{window}_min" + feature[3:]
                desired_features_list.append(new_feature)
    

    
    return all_features_daily_dataframe[desired_features_list]


def engineer_features(adjusted_1min_data, final_news_dataset):
    # Ensure the output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    final_dataset ={}
    stocks_list, desired_features_list,  days_to_news, preferred_parameters = read_config(CONFIG_PATH)
    desired_features_list.append('date')
    for stock in stocks_list:
        new_df = adjusted_1min_data[stock]
        enhanced_1min_dataframe = prepare_enhanced_1min_dataframe(new_df, preferred_parameters)
        base_daily_dataframe = aggregate_daily(enhanced_1min_dataframe, preferred_parameters)
        all_features_daily_dataframe = build_sophisticated_daily(base_daily_dataframe, preferred_parameters)
        selected_features_dataframe = select_features(all_features_daily_dataframe, desired_features_list, preferred_parameters)
        all_features_daily_dataframe_plus_lable = add_label(selected_features_dataframe, final_news_dataset[stock], days_to_news )
        final_dataset[stock] = all_features_daily_dataframe_plus_lable


    generate_output(stocks_list, final_dataset)