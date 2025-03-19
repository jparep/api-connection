from fetch_cdc_data import fetch_cdc_data
from load_to_snowflake import load_data_to_snowflake

if __name__ == "__main__":
    print("Starting CDC Pipeline...")
    data = fetch_cdc_data()
    
    if data:
        print("CDC Data Fetched Successfully. Proceeding to Load.")
        load_data_to_snowflake(data)
    else:
        print("No data received. Aborting load process.")
