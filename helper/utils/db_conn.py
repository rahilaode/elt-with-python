import psycopg2
import warnings
warnings.filterwarnings('ignore')

def db_connection():
    """
    Establishes connections to source and data warehouse (DWH) databases.

    Returns:
        tuple: A tuple containing the following elements:
            - psycopg2.extensions.connection: Connection object for the source database.
            - psycopg2.extensions.cursor: Cursor object for the source database.
            - psycopg2.extensions.connection: Connection object for the data warehouse database.
            - psycopg2.extensions.cursor: Cursor object for the data warehouse database.

    Note:
        Make sure to handle exceptions properly in the calling code.
        This function assumes that PostgreSQL is running locally on default ports.
        Update the connection parameters (database, host, user, password, port)
        according to your PostgreSQL server configuration.
    """
    try:
        conn_src = psycopg2.connect(database="mini_order",
                                    host="localhost",
                                    user="postgres",
                                    password="mypassword",
                                    port="5433")
        cur_src = conn_src.cursor()

        conn_dwh = psycopg2.connect(database="dwh",
                                    host="localhost",
                                    user="postgres",
                                    password="mypassword",
                                    port="5434")
        cur_dwh = conn_dwh.cursor()
        
        return conn_src, cur_src, conn_dwh, cur_dwh

    except Exception as e:
        print(f"Error: {e}")
        return None
