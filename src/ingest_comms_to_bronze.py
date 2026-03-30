import os
import json
import requests
import time
from datetime import datetime, timezone
import logging
from config import config

logger = logging.getLogger(__name__)

os.makedirs(config.bronze_path, exist_ok=True)
BASE_URL = "https://api.congress.gov/v3/member"


def fetch_legislator_data():
    logger.info("Starting data ingestion from Congress API...")

    query_params = {
        "api_key": config.congress_api_key.get_secret_value(),
        "format": "json",
        "currentMember": "true"
    } 

    try:
        logger.info("Requesting data from BASE_URL: %s", BASE_URL)
        response = requests.get(BASE_URL, timeout=10, params=query_params)
        response.raise_for_status()
        # Parse the response as JSON
        data = response.json()

        members_count = len(data.get("members", []))
        logger.info("Success retrieving %s members from API", members_count)

        # Get the current date and time in UTC timezone and Unix timestamp format
        now_utc = datetime.now(timezone.utc)
        current_date = now_utc.strftime("%Y-%m-%d")
        partition_date = f"ingested_at={current_date}"
        unix_ts = int(time.time())

        # Create the full directory path with the partition date
        full_dir_path = os.path.join(config.bronze_path, partition_date)
        os.makedirs(full_dir_path, exist_ok=True)

        file_name = f"raw_comms_{unix_ts}.json"
        full_file_path = os.path.join(full_dir_path, file_name)

        with open(full_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logger.info("Data ingestion from Congress API completed successfully. Saved to: %s", full_file_path)
        return full_file_path
    
    except requests.exceptions.RequestException as e:
        logger.error("Critical error during data ingestion: %s", str(e))
        raise