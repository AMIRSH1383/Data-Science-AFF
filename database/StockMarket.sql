-- 0: Creating database
DROP DATABASE IF EXISTS Stock_Market;
CREATE DATABASE Stock_Market;
USE Stock_Market;

-- 1: Financial news
DROP TABLE IF EXISTS financial_news;
CREATE TABLE financial_news (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_name VARCHAR(50),
    news_title TEXT,
    news_code VARCHAR(20),
    date_time DATETIME
);

-- 2: Stocks 1m 
DROP TABLE IF EXISTS stock_1m;
CREATE TABLE stock_1m (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_name VARCHAR(50),
    date_time DATETIME,
    volume FLOAT,
    open_price FLOAT,
    highest_price FLOAT,
    lowest_price FLOAT,
    close_price FLOAT,
    avg_price FLOAT,
    trade_number INT,
    target INT,
    days_to_news INT
);

-- 3: Stock daily
DROP TABLE IF EXISTS stock_daily;
CREATE TABLE stock_daily (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_name VARCHAR(50),
    transaction_date DATE,
    target INT,
    days_to_news INT,
    open_price FLOAT,
    highest_price FLOAT,
    lowest_price FLOAT,
    close_price FLOAT,
    vol FLOAT,
    signed_volume FLOAT,
    zero_vol_mins_count INT,
    significant_mins_count INT,
    max_signed_volume FLOAT,
    trade_value FLOAT,
    trade_value_std FLOAT,
    RSI_above_80_count INT,
    RSI_below_20_count INT,
    avg_price FLOAT,
    max_box_cross_2p_count INT,
    price_cross_bb INT,
    trade_value_30d_avg FLOAT,
    trade_val_ratio FLOAT,
    body_vs_shadow FLOAT,
    interaday_volatility FLOAT,
    cloud_width_to_range FLOAT,
    max_10_min_price_change_pos FLOAT,
    max_10_min_price_change_neg FLOAT,
    max_10_min_trade_Value FLOAT,
    correlation FLOAT,
    RSI FLOAT
);


