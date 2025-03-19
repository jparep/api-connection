import requests
import json
from config import API_URL, API_KEY

def fetch_cdc_data():
    """Fetch Change Data Capture (CDC) data from API."""
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        response.raise_for_status()  # Raise HTTP errors
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

if __name__ == "__main__":
    data = fetch_cdc_data()
    if data:
        with open("cdc_data.json", "w") as file:
            json.dump(data, file, indent=4)
        print("CDC data saved to cdc_data.json")
