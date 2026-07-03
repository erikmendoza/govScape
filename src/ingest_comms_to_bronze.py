import json
import requests
import time
from datetime import datetime, timezone
import logging
from config import config

logger = logging.getLogger(__name__)

config.bronze_path.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://api.congress.gov/v3/member"


def fetch_legislator_data():
    logger.info("Starting data ingestion from Congress API...")

    all_members = []
    limit = 250
    offset = 0
    unix_ts = int(time.time())
    now_utc = datetime.now(timezone.utc)
    current_date = now_utc.strftime("%Y-%m-%d")
    partition_date = f"ingested_at={current_date}"

    try:
        while True:
            logger.info("Staring pagination with limit=%s and offset=%s", limit, offset)
            query_params = {
                "api_key": config.congress_api_key.get_secret_value(),
                "format": "json",
                "currentMember": "true",
                "limit": limit,
                "offset": offset,
            }

            response = requests.get(BASE_URL, timeout=30, params=query_params)
            response.raise_for_status()
            data = response.json()
            current_batch = data.get("members", [])

            if not current_batch:
                break

            all_members.extend(current_batch)

            pagination_info = data.get("pagination", {})

            if "next" not in pagination_info or len(current_batch) < limit:
                break

            offset += limit
            time.sleep(2)

        if not all_members:
            logger.warning("No legislators returned from Congress API.")
            return None

        file_name = f"raw_comms_{unix_ts}.json"
        full_file_path = config.bronze_path / "legislators_comms" / partition_date / file_name
        logger.info(f"Saving data to {full_file_path}")
        data = {"members": all_members, "pagination": {"count": len(all_members)}}
        full_file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(full_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logger.info(f"Success retrieving {len(all_members)} legislators.")
        return full_file_path

    except requests.exceptions.RequestException as e:
        logger.error("Critical error during data ingestion: %s", str(e))
        raise


if __name__ == "__main__":
    fetch_legislator_data()
