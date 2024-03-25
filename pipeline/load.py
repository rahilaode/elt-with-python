from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker
import sqlalchemy
import pandas as pd
import os

def load():
    try:
        
        # Define db connection engine
        _, dwh_engine = db_connection()
        
        # Define DIR
        DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
        DIR_LOAD_QUERY = os.getenv("DIR_LOAD_QUERY")
        
        #-------------------------------Truncate tables in public schema ------------------------
        # Read query to truncate public schema in dwh
        truncate_query = read_sql_file(
            file_path = f'{DIR_LOAD_QUERY}/public-truncate_tables.sql'
        )  
        
        # Split the SQL queries if multiple queries are present
        truncate_query = truncate_query.split(';')

        # Remove newline characters and leading/trailing whitespaces
        truncate_query = [query.strip() for query in truncate_query if query.strip()]
        
        # Create session
        Session = sessionmaker(bind = dwh_engine)
        session = Session()

        # Execute each query
        for query in truncate_query:
            query = sqlalchemy.text(query)
            session.execute(query)
        
        # Commit the transaction
        session.commit()
        
        # Close session
        session.close()      

        #-------------------------------Part Of Load to public schema ------------------------
        # Data to be loaded into public schema
        category = pd.read_csv(f'{DIR_TEMP_DATA}/public.category.csv')
        subcategory = pd.read_csv(f'{DIR_TEMP_DATA}/public.subcategory.csv')
        customer = pd.read_csv(f'{DIR_TEMP_DATA}/public.customer.csv')
        orders = pd.read_csv(f'{DIR_TEMP_DATA}/public.orders.csv')
        product = pd.read_csv(f'{DIR_TEMP_DATA}/public.product.csv')
        order_detail = pd.read_csv(f'{DIR_TEMP_DATA}/public.order_detail.csv')
        
        # Load to public schema
        # Load category tables    
        category.to_sql('category', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'public')
        
        # Load subcategory tables
        subcategory.to_sql('subcategory', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'public')
        
        
        # Load customer tables
        customer.to_sql('customer', 
                        con = dwh_engine, 
                        if_exists = 'append', 
                        index = False, 
                        schema = 'public')
        
        # Load orders tables
        orders.to_sql('orders', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
        
        
        # Load product tables
        product.to_sql('product', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
        
        
        # Load order_detail tables
        order_detail.to_sql('order_detail', 
                    con = dwh_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
        
        
        #-------------------------------Part Of Load to staging schema ------------------------
        # Read load query to staging schema
        category_query = read_sql_file(
            file_path = f'{DIR_LOAD_QUERY}/stg-category.sql'
        )
        
        subcategory_query = read_sql_file(
            file_path = f'{DIR_LOAD_QUERY}/stg-subcategory.sql'
        )
        
        customer_query = read_sql_file(
            file_path = f'{DIR_LOAD_QUERY}/stg-customer.sql'
        )
        
        orders_query = read_sql_file(
            file_path = f'{DIR_LOAD_QUERY}/stg-orders.sql'
        )
        
        product_query = read_sql_file(
            file_path = f'{DIR_LOAD_QUERY}/stg-product.sql'
        )
        
        order_detail_query = read_sql_file(
            file_path = f'{DIR_LOAD_QUERY}/stg-order_detail.sql'
        )
        
        ## Load Into Staging scheama
        # List query
        load_stg_queries = [category_query, subcategory_query, customer_query,
                            orders_query, product_query, order_detail_query]
        
        # Create session
        Session = sessionmaker(bind = dwh_engine)
        session = Session()

        # Execute each query
        for query in load_stg_queries:
            query = sqlalchemy.text(query)
            session.execute(query)
            
        session.commit()
        
        # Close session
        session.close()
        
        
    except Exception as e:
        print(f"Error loading data: {e}")
        
# Execute the functions when the script is run
if __name__ == "__main__":
    load()