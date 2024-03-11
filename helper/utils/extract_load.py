import pandas as pd
from db_conn import db_connection
import warnings
warnings.filterwarnings('ignore')


def get_table_data(table_name, engine):
    """
    Retrieves data from a specified table in a SQL database and returns it as a pandas DataFrame.

    Args:
        table_name (str): The name of the table from which to retrieve data.
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine object connected to the database.

    Returns:
        pandas.DataFrame: A DataFrame containing the data from the specified table.

    Notes:
        Ensure that the engine provided is properly configured and connected to the database.
        Handle exceptions properly in the calling code.
        If an error occurs during data retrieval, an empty DataFrame will be returned.
    """
    try:
        # Construct SQL query to select all data from the specified table
        query = f"SELECT * FROM {table_name};"

        # Execute the query and read the results into a DataFrame
        df = pd.read_sql(query, engine)
        
        return df

    except Exception as e:
        # Print the error message if an exception occurs
        print(f"Error: {e}")
        
        # Return an empty DataFrame in case of an error
        return pd.DataFrame()


def compare(df1, df2):
    """
    Compares two pandas DataFrames and returns rows that are unique to each DataFrame.

    Args:
        df1 (pandas.DataFrame): The first DataFrame to be compared.
        df2 (pandas.DataFrame): The second DataFrame to be compared.

    Returns:
        pandas.DataFrame: A DataFrame containing rows that are unique to either df1 or df2.

    Notes:
        Both df1 and df2 should have the same column structure.
        The function concatenates df1 and df2, then drops duplicate rows, 
        keeping only rows that are unique to each DataFrame.
    """
    try:
        df = pd.concat([df1, df2])
        df = df.drop_duplicates(keep=False)
        
        return df

    except Exception as e:
        print(f"Error: {e}")
        
        return pd.DataFrame()

def extract_load():
    """
    Extracts data from source and DWH databases, compares them, and loads unique data into the staging area.

    Returns:
        None: This function does not return any value.

    Notes:
        - This function assumes that appropriate connections to source and DWH databases are already established.
        - It extracts data from various tables from both source and DWH databases.
        - It compares data between source and DWH databases to identify unique records.
        - It loads unique records into the staging area tables.
        - Make sure to handle exceptions properly in the calling code.
    """
    try:
        # Establish connections to source and DWH databases
        conn_src, cur_src, conn_dwh, cur_dwh = db_connection()
        
        # Extract from data sources
        src_category = get_table_data('category', conn_src)
        src_customer = get_table_data('customer', conn_src)
        src_order_detail = get_table_data('order_detail', conn_src)
        src_orders = get_table_data('orders', conn_src)
        src_product = get_table_data('product', conn_src)
        src_subcategory = get_table_data('subcategory', conn_src)
        
        # Extract from DWH (staging)
        stg_category = get_table_data('stg.category', conn_dwh).drop('uuid', axis=1)
        stg_customer = get_table_data('stg.customer', conn_dwh).drop('uuid', axis=1)
        stg_order_detail = get_table_data('stg.order_detail', conn_dwh).drop('uuid', axis=1)
        stg_orders = get_table_data('stg.orders', conn_dwh).drop('uuid', axis=1)
        stg_product = get_table_data('stg.product', conn_dwh).drop('uuid', axis=1)
        stg_subcategory = get_table_data('stg.subcategory', conn_dwh).drop('uuid', axis=1)
        
        # Get the difference data
        category = compare(src_category, stg_category)
        customer = compare(src_customer, stg_customer)
        order_detail = compare(src_order_detail, stg_order_detail)
        orders = compare(src_orders, stg_orders)
        product = compare(src_product, stg_product)
        subcategory = compare(src_subcategory, stg_subcategory)
        
        # Load to 'category' Table
        for index, row in category.iterrows():
            # Extract values from the DataFrame row
            name = row['name']
            description = row['description']
            created_at = row['created_at']
            updated_at = row['updated_at']

            # Construct the SQL INSERT query
            insert_query = f"""
            INSERT INTO stg.category 
            (name, description, created_at, updated_at) 
            VALUES 
            ('{name}', '{description}', '{created_at}', '{updated_at}');
            """

            # Execute the INSERT query
            cur_dwh.execute(insert_query)

        # Commit the transaction
        conn_dwh.commit()
        
        # Load to 'subcategory' table
        for index, row in subcategory.iterrows():
            # Extract values from the DataFrame row
            name = row['name']
            category_id = row['category_id']
            description = row['description']
            created_at = row['created_at']
            updated_at = row['updated_at']
            
            
            # Construct the SQL INSERT query
            insert_query = f"""
            INSERT INTO stg.subcategory 
            (name, category_id, description, created_at, updated_at) 
            VALUES 
            ('{name}', '{category_id}', '{description}', '{created_at}', '{updated_at}');
            """

            # Execute the INSERT query
            cur_dwh.execute(insert_query)

        # Commit the transaction
        conn_dwh.commit()
        
        # Load to 'customer' table
        for index, row in customer.iterrows():
            # Extract values from the DataFrame row
            first_name = row['first_name']
            last_name = row['last_name']
            email = row['email']
            phone = row['phone']
            address = row['address']
            created_at = row['created_at']
            updated_at = row['updated_at']

            # Construct the SQL INSERT query
            insert_query = f"""
            INSERT INTO stg.customer 
            (first_name, last_name, email, phone, address, created_at, updated_at) 
            VALUES 
            ('{first_name}', '{last_name}', '{email}', '{phone}', '{address}', '{created_at}', '{updated_at}');
            """

            # Execute the INSERT query
            cur_dwh.execute(insert_query)

        # Commit the transaction
        conn_dwh.commit()
        
        # Load to 'orders' table
        for index, row in orders.iterrows():
            # Extract values from the DataFrame row
            order_id = row['order_id']
            customer_id = row['customer_id']
            order_date = row['order_date']
            status = row['status']
            created_at = row['created_at']
            updated_at = row['updated_at']

            # Construct the SQL INSERT query
            insert_query = f"""
            INSERT INTO stg.orders 
            (order_id, customer_id, order_date, status, created_at, updated_at) 
            VALUES 
            ('{order_id}', '{customer_id}', '{order_date}', '{status}', '{created_at}', '{updated_at}');
            """

            # Execute the INSERT query
            cur_dwh.execute(insert_query)

        # Commit the transaction
        conn_dwh.commit()
        
        # Load to 'product' table
        for index, row in product.iterrows():
            # Extract values from the DataFrame row
            product_id = row['product_id']
            name = row['name']
            subcategory_id = row['subcategory_id']
            price = row['price']
            stock = row['stock']
            created_at = row['created_at']
            updated_at = row['updated_at']

            # Construct the SQL INSERT query
            insert_query = f"""
            INSERT INTO stg.product 
            (product_id, name, subcategory_id, price, stock, created_at, updated_at) 
            VALUES 
            ('{product_id}', '{name}', {subcategory_id}, {price}, {stock}, '{created_at}', '{updated_at}');
            """

            # Execute the INSERT query
            cur_dwh.execute(insert_query)

        # Commit the transaction
        conn_dwh.commit()
        
        # Load to 'order_detail' table
        for index, row in order_detail.iterrows():
            # Extract values from the DataFrame row
            order_id = row['order_id']
            product_id = row['product_id']
            quantity = row['quantity']
            price = row['price']
            created_at = row['created_at']
            updated_at = row['updated_at']

            # Construct the SQL INSERT query
            insert_query = f"""
            INSERT INTO stg.order_detail 
            (order_id, product_id, quantity, price, created_at, updated_at) 
            VALUES 
            ('{order_id}', '{product_id}', '{quantity}', '{price}', '{created_at}', '{updated_at}');
            """

            # Execute the INSERT query
            cur_dwh.execute(insert_query)

        # Commit the transaction
        conn_dwh.commit()
        
        # Close the cursor and connection
        conn_src.close()
        cur_src.close()
        conn_dwh.close()
        cur_dwh.close()

    except Exception as e:
        print(f"Error extracting and loading data: {e}")
        
# Execute the functions when the script is run
if __name__ == "__main__":
    extract_load()