import os
import json
import requests
import time
from datetime import datetime, timezone

BRONZE_PATH = "data/bronze/legislators_comms"
os.makedirs(BRONZE_PATH, exist_ok=True)


def fetch_legislator_data():
    print("Start fetching legislators")
    # Make a GET request to the API endpoint
    url = "https://rickandmortyapi.com/api/character"
    response = requests.get(url, timeout=10)
    #response.raise_for_status()
    if response.status_code == 200:
        # Parse the response as JSON
        data = response.json()
        # Get the current date and time in UTC timezone and Unix timestamp format
        now_utc = datetime.now(timezone.utc)
        current_date = now_utc.strftime("%Y-%m-%d")
        partition_date = f"ingested_at={current_date}"
        unix_ts = int(time.time())

        # Create the full directory path with the partition date
        full_dir_path = os.path.join(BRONZE_PATH, partition_date)
        os.makedirs(full_dir_path, exist_ok=True)

        file_name = f"raw_comms_{unix_ts}.json"
        full_file_path = os.path.join(full_dir_path, file_name)

        with open(full_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"File saved successfully: {full_file_path}")
        return full_file_path
    else:
        print("Error: API request failed")
        return None


if __name__ == "__main__":
    raw = fetch_legislator_data()