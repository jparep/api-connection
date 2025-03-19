import snowflake.connector
import json
from config import (
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE
)

def load_data_to_snowflake():
    """Load saved CDC JSON data into Snowflake RAW table."""
    try:
        # Read JSON Data
        with open("cdc_data.json", "r") as file:
            data = json.load(file)
        
        if not data:
            print("No data found in cdc_data.json")
            return

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT
        )

        cursor = conn.cursor()

        # Create table if not exists
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
            id STRING,
            payload VARIANT,
            timestamp TIMESTAMP
        );
        """
        cursor.execute(create_table_query)

        # Insert Data
        insert_query = f"""
        INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (id, payload, timestamp)
        SELECT 
            PARSE_JSON(column1):id, 
            PARSE_JSON(column1):data, 
            PARSE_JSON(column1):timestamp 
        FROM VALUES {', '.join([f'(\'' + json.dumps(record) + '\')' for record in data])}
        """
        
        cursor.execute(insert_query)
        conn.commit()
        print("Data successfully loaded into Snowflake.")

    except Exception as e:
        print(f"Error loading data: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_data_to_snowflake()
