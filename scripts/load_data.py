from database_connection import engine
import pandas as pd

df = pd.read_sql("SELECT * FROM stock_daily", con=engine)