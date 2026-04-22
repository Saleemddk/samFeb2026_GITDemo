import oracledb

try:
    # Connect to Oracle Database
    connection = oracledb.connect(
        user='test_user',
        password='Admin123',
        dsn='localhost:1521/XEPDB1'
    )
    
    if connection:
        cursor = connection.cursor()
        
        print("✓ Connected to Oracle Database successfully!")
        print(f"Database: XEPDB1")
        print(f"User: test_user")
        
        # Get row count from sales table
        try:
            cursor.execute("SELECT COUNT(*) FROM sales")
            row_count = cursor.fetchone()[0]
            print(f"\n--- Sales Table Information ---")
            print(f"Row Count: {row_count}")
        except Exception as e:
            print(f"Error querying sales table: {e}")
        
        # Get table structure
        try:
            cursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME='SALES' ORDER BY COLUMN_ID")
            columns = cursor.fetchall()
            
            if columns:
                print(f"\nColumns in SALES table:")
                for col in columns:
                    print(f"  - {col[0]}: {col[1]}")
            else:
                print("No columns found or table does not exist")
        except Exception as e:
            print(f"Error retrieving table structure: {e}")

except Exception as e:
    print(f"Error: {e}")

finally:
    if connection:
        cursor.close()
        connection.close()
        print("\nOracle connection closed.")
