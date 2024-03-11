from db_conn import db_connection
import warnings
warnings.filterwarnings('ignore')

def transform():
    """
    Transforms data from staging tables to dimension and fact tables in the data warehouse (DWH).

    Returns:
        None: This function does not return any value.

    Notes:
        - This function assumes that appropriate connections to source and DWH databases are already established.
        - It transforms data from staging tables to dimension and fact tables in the DWH.
        - Ensure that the SQL queries within the function are correctly formulated according to the database schema.
        - Make sure to handle exceptions properly in the calling code.
    """
    try:
        # Establish connections to source and DWH databases
        conn_src, cur_src, conn_dwh, cur_dwh = db_connection()
        
        # dim_customer
        # Construct the SQL INSERT query
        insert_query = f"""
        INSERT INTO prod.dim_customer (
            customer_id,
            customer_nk,
            first_name,
            last_name,
            email,
            phone,
            address,
            created_at,
            updated_at
        )

        SELECT
            c.uuid AS customer_id,
            c.customer_id AS customer_nk,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            c.address,
            c.created_at,
            c.updated_at
        FROM
            stg.customer c
            
        WHERE NOT EXISTS (
            SELECT 1
            FROM prod.dim_customer t
            WHERE t.first_name = c.first_name
            AND t.last_name = c.last_name 
            AND t.email = c.email
            AND t.phone = c.phone 
            AND t.address = c.address
            AND t.created_at = c.created_at
            AND t.updated_at = c.updated_at
        );
        """

        # Execute the INSERT query
        cur_dwh.execute(insert_query)

        # Commit the transaction
        conn_dwh.commit()
        
        
        
        # dim_product
        # Construct the SQL INSERT query
        insert_query = f"""
        INSERT INTO prod.dim_product (
            product_id,
            product_nk,
            "name",
            price,
            stock,
            category_name,
            category_desc,
            subcategory_name,
            subcategory_desc,
            created_at,
            updated_at
        )

        SELECT
            p.uuid AS product_id,
            p.product_id AS product_nk,
            p."name",
            p.price,
            p.stock,
            c."name" AS category_name,
            c.description AS category_desc,
            s."name" AS subcategory_name,
            s.description AS subcategory_desc,
            p.created_at,
            p.updated_at
        FROM
            stg.product p
            
        INNER JOIN
            stg.subcategory s ON p.subcategory_id = s.subcategory_id
        INNER JOIN
            stg.category c ON s.category_id = c.category_id
            
        WHERE NOT EXISTS (
            SELECT 1
            FROM prod.dim_product t
            WHERE t."name" = p."name"
            AND t.price = p.price
            AND t.stock = p.stock
            AND t.category_name = c."name"
            AND t.category_desc = c.description
            AND t.subcategory_name = s."name"
            AND t.subcategory_desc = s.description
            AND t.created_at = p.created_at
            AND t.updated_at = p.updated_at
        );
        """

        # Execute the INSERT query
        cur_dwh.execute(insert_query)

        # Commit the transaction
        conn_dwh.commit()
        
        
        
        # fact_order
        # Construct the SQL INSERT query
        insert_query = f"""
        INSERT INTO prod.fact_order (
            order_id,
            product_id,
            customer_id,
            order_date,
            quantity,
            status,
            created_at,
            updated_at
        )

        SELECT
            od.uuid as order_id,
            dp.product_id,
            dc.customer_id,
            dd.date_id as order_date,
            od.quantity,
            o.status,
            o.created_at,
            o.updated_at
        FROM
            stg.order_detail od
            
        INNER join 
            stg.orders o ON od.order_id = o.order_id
        INNER join 
            prod.dim_customer dc ON o.customer_id = dc.customer_nk
        INNER join 
            prod.dim_product dp on od.product_id = dp.product_nk 
        INNER join 
            prod.dim_date dd on o.order_date = dd.date_actual
            
        WHERE NOT EXISTS (
            SELECT 1
            FROM prod.fact_order t
            WHERE t."order_id" = od.uuid
            AND t.product_id = dp.product_id
            AND t.customer_id = dc.customer_id
            AND t.order_date = dd.date_id
            AND t.quantity = od.quantity
            AND t.status = o.status
            AND t.created_at = o.created_at
            AND t.updated_at = o.updated_at
        );
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
        print(f"Error during data transformation: {e}")
        
# Execute the functions when the script is run
if __name__ == "__main__":
    transform()