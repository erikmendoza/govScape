# config.py
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("CONGRESS_API_KEY")

# Paths
BASE_DATA_PATH = "data"
BRONZE_PATH = os.path.join(BASE_DATA_PATH, "bronze/legislators_comms")
SILVER_PATH = os.path.join(BASE_DATA_PATH, "silver/legislators_comms")
GOLD_PATH = os.path.join(BASE_DATA_PATH, "gold/metrics")

# ==========================================
# 1. CONFIGURATION & EXPECTATIONS
# ==========================================
CRITICAL_MIN_RECORDS = int(os.getenv("CRITICAL_MIN_RECORDS", 5))
EXPECTED_MIN_STATES = int(os.getenv("EXPECTED_MIN_STATES", 5))
MANDATORY_COLUMNS = ['bioguideId', 'state']
OPTIONAL_COLUMNS = ['name', 'partyName']

# Logging
LOG_FILE = "govscape_pipeline.log"
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'