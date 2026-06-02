import json
import logging
import time
from datetime import datetime, timezone
import requests
from config import config

logger = logging.getLogger(__name__)

BASE_URL = "https://api.congress.gov/v3/bill"


def fetch_bills_data():
    logger.info("Starting data ingestion from Congress API...")

    query_params = {
        "api_key": config.congress_api_key.get_secret_value(),
        "format": "json",
        "limit": 250,
    }
    try:
        logger.info("Requesting data from BASE_URL: %s with limit=250", BASE_URL)

        response = requests.get(BASE_URL, timeout=15, params=query_params)
        response.raise_for_status()

        data = response.json()
        bills_count = len(data.get("bills", []))
        logger.info("Success retrieving %s bills from API", bills_count)

        now_utc = datetime.now(timezone.utc)
        current_date = now_utc.strftime("%Y-%m-%d")
        partition_date = f"ingested_at={current_date}"
        unix_ts = int(time.time())

        full_dir_path = config.bronze_path / "bills" / partition_date
        full_dir_path.mkdir(parents=True, exist_ok=True)

        file_name = f"raw_bills_{unix_ts}.json"
        full_file_path = full_dir_path / file_name

        with open(full_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logger.info("Data ingestion completed successfully")

        return full_file_path

    except Exception as e:
        logger.error(f"Error fetching bills data: {e}")
