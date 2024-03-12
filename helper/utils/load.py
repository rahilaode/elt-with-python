from db_conn import db_connection
import pandas as pd

def load():
    """
    Load data from CSV files into the staging tables of the data warehouse (DWH).

    Returns:
    None

    Raises:
    Any exceptions raised by the underlying functions/db_connection, pd.read_csv,
    or database operations (INSERT queries and commits).
    """
    try:
        # Data to be loaded
        category = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/category.csv')
        subcategory = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/subcategory.csv')
        customer = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/customer.csv')
        orders = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/orders.csv')
        product = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/product.csv')
        order_detail = pd.read_csv('/home/laode/pacmann/project/elt-with-python/helper/utils/temp_data/order_detail.csv')
        
        # Establish connections to source and DWH databases
        conn_src, cur_src, conn_dwh, cur_dwh = db_connection()
        
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
        print(f"Error loading data: {e}")
        
# Execute the functions when the script is run
if __name__ == "__main__":
    load()