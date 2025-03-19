import requests
import json
from config import API_URL, API_KEY

def fetch_cdc_data():
    """Fetch CDC data from API without storing it in a file."""
    headers = {"Authorization": f"Bearer {API_KEY}"}

    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()  # Directly return JSON data (No file storage)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
