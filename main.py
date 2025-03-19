from fetch_cdc_data import fetch_cdc_data
from load_to_snowflake import load_data_to_snowflake

if __name__ == "__main__":
    print("Starting CDC Pipeline...")
    data = fetch_cdc_data()
    
    if data:
        with open("cdc_data.json", "w") as file:
            json.dump(data, file, indent=4)
        print("CDC Data Fetched Successfully. Proceeding to Load.")
        
        load_data_to_snowflake()
    else:
        print("No data received. Aborting load process.")
