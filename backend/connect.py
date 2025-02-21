# Third-party imports
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Local imports
from backend import config

# Database Connection

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None

    try:
        # read connection parameters
        params = config.get_credpulse_db_config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        connection_string = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"

        engine = create_engine(connection_string)
        
        conn = engine.connect()

        # display the PostgreSQL database server version
        result = conn.execute(text("SELECT version();"))
        version = result.fetchone()
        
        print(f"PostgreSQL version: {version[0]}")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            print("Database Connection Established...")
            return engine, params
        

if __name__ == '__main__':
    connect()