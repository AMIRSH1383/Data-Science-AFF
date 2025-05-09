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

## 🗂️ Project Structure

```
.
├── pipeline.py                      # Main entry point for running the pipeline
├── scripts/
│   ├── import_to_DB.py              # Load raw csv filed into tables
│   ├── database_connection.py       # Function to connect to MySQL using SQLAlchemy
│   ├── load_data.py                 # SQL queries and data loading logic
│   ├── preprocess.py                # Data cleaning and alignment functions
│   └── feature_engineering.py       # Core feature engineering logic
├── requirements.txt                 # Python package dependencies
├── .github/workflows/               # CI pipeline (GitHub Actions)
│   └── pipeline.yml
└── README.md                        # This file
```

## 🧪 Running the Pipeline

Before running the pipeline, ensure:

- MySQL is running and contains the required tables (`stock_1m`, `stock_daily`, `financial_news`).
- Run import_to_db.py to fill your database with datas.
- Add datas to database folder.
- Connection credentials in `scripts/database_connection.py` are correct.

### ✅ Step-by-step (Locally)
```bash
pip install -r requirements.txt
python pipeline.py
```

### 🧪 GitHub Actions
The pipeline runs automatically on push to the `main` branch, using a Dockerized MySQL service and GitHub-hosted runners.

## 📁 Output

The output datasets are saved in the action output and include:

- `final_dataset.csv`: Full merged dataset with all stocks
- `[stock_name]final_daily.csv`: Per-stock processed datasets
- `test_extreme.csv`: Debug file from feature engineering (optional)

## ⚙️ Requirements

- Python 3.10
- MySQL Server 8.0+
- See `requirements.txt` for Python dependencies.

## ✍️ Author

Amirarsalan – [GitHub Profile](https://github.com/AMIRSH1383)

## 📝 License

MIT License – see `LICENSE` file for details.
