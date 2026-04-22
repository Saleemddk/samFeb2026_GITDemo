import mysql.connector
from mysql.connector import Error

try:
    # Connect to MySQL
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Admin123'
    )
    
    if connection.is_connected():
        cursor = connection.cursor()
        
        # Create the schema if it doesn't exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS retail_banking")
        cursor.execute("USE retail_banking")
        
        # Create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS git_sales (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sales DECIMAL(10, 2)
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        
        print("✓ Table created successfully!")
        
        # Get table count
        cursor.execute("SELECT COUNT(*) FROM git_sales")
        row_count = cursor.fetchone()[0]
        
        # Get table name
        cursor.execute("SHOW TABLES LIKE 'git_sales'")
        table_name = cursor.fetchone()
        
        print("\n--- Table Information ---")
        print(f"Table Name: {table_name[0]}")
        print(f"Row Count: {row_count}")
        print(f"Schema: retail_banking")

except Error as e:
    print(f"Error: {e}")

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("\nMySQL connection closed.")
