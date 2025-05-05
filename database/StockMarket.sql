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
    trade_number INT
);

-- 3: Stock daily
DROP TABLE IF EXISTS stock_1d;
CREATE TABLE stock_daily (
    id INT AUTO_INCREMENT PRIMARY KEY,
    aggregated BOOLEAN,
    stock_name VARCHAR(50),
    date_time VARCHAR(50),
    volume FLOAT,
    open_price FLOAT,
    highest_price FLOAT,
    lowest_price FLOAT,
    close_price FLOAT
);

