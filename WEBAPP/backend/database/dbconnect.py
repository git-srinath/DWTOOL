import os
import oracledb
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Oracle database configuration
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_sid = os.getenv('DB_SID')

# SQLite configuration for user management
sqlite_engine = create_engine(os.getenv('SQLITE_DATABASE_URL'))

def create_oracle_connection():
    try:
        connection = oracledb.connect(
            user=db_user,
            password=db_password,
            dsn=f"{db_host}:{db_port}/{db_sid}"
        )
        print("Oracle connection established successfully")
        return connection
    except Exception as e:
        print(f"Error establishing Oracle connection: {str(e)}")
        raise 



def create_oracle_connection_dwapp():
    try:
        connection = oracledb.connect(
            user=db_user,
            password=db_password,
            dsn=f"{db_host}:{db_port}/{db_sid}"
        )
        print("Oracle connection established successfully")
        return connection
    except Exception as e:
        print(f"Error establishing Oracle connection: {str(e)}")
        raise 