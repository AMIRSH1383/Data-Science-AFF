# 📊 Stock Market Data Pipeline

This project implements a complete pipeline for processing, analyzing, and engineering features from stock market and financial news data stored in a MySQL database.

## 🚀 Pipeline Overview

The main pipeline is run through `pipeline.py`, which performs the following steps:

1. **Database Connection**  
   Connects to a local or containerized MySQL database via SQLAlchemy.

2. **Data Loading**  
   Fetches raw minute-level and daily-level stock data, along with related financial news.

3. **Preprocessing**  
   Cleans and aligns the data for consistency, handling missing values and outliers.

4. **Feature Engineering**  
   Extracts technical and statistical features to prepare the dataset for machine learning or financial analysis.

## 🧪 Running the Pipeline

Before running the pipeline, ensure:

- MySQL is running and contains the required tables (`stock_1m`, `stock_daily`, `financial_news`).
- Run import_to_db.py to fill your database with datas.
- Add datas of the database to database folder.
- Connection credentials in `scripts/database_connection.py` are correct.

### ✅ Step-by-step (Locally)
```bash
pip install -r requirements.txt
python pipeline.py
