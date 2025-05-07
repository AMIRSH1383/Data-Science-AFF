from sqlalchemy import create_engine

# MySQL credentials
# USERNAME = "Amirarsalan"
USERNAME = 'root'
PASSWORD = 'Farjad83'
HOST = "localhost"
PORT = "3306"
DATABASE = "stock_market"

# Create the SQLAlchemy engine
# CONNECTION_STRING = f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
CONNECTION_STRING = f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

engine = create_engine(CONNECTION_STRING)



def get_connection():
    return engine


