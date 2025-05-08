# from sqlalchemy import create_engine

# # MySQL credentials
# # USERNAME = "Amirarsalan"
# USERNAME = 'root'
# PASSWORD = 'Farjad83'
# HOST = "localhost"
# PORT = "3306"
# DATABASE = "stock_market"

# # Create the SQLAlchemy engine
# # CONNECTION_STRING = f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
# CONNECTION_STRING = f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

# engine = create_engine(CONNECTION_STRING)

from sqlalchemy import create_engine
import os

user = os.getenv("DB_USER", "Amirarsalan")
password = os.getenv("DB_PASS", "")
host = os.getenv("DB_HOST", "localhost")
database = os.getenv("DB_NAME", "stock_market")

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")


def get_connection():
    return engine




