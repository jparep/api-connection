import snowflake.connector
import json
from config import (
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE
)

def load_data_to_snowflake(data):
    """Load CDC JSON data directly into Snowflake without creating tables dynamically."""
    if not data:
        print("No data to load.")
        return

    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT
        )

        cursor = conn.cursor()

        # Prepare Data for Insertion
        values_list = ", ".join([
            f"('{json.dumps(record)}')" for record in data
        ])

        insert_query = f"""
        INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (id, payload, timestamp)
        SELECT 
            PARSE_JSON(column1):id, 
            PARSE_JSON(column1):data, 
            PARSE_JSON(column1):timestamp 
        FROM VALUES {values_list}
        """

        cursor.execute(insert_query)
        conn.commit()
        print("Data successfully loaded into Snowflake.")

    except Exception as e:
        print(f"Error loading data: {e}")

    finally:
        cursor.close()
        conn.close()
