import pandas as pd
import json
import os
import logging
from config import config

logger = logging.getLogger(__name__)

# ==========================================
# 2. DATA TRANSFORMATION LOGIC
# ==========================================


def clean_legislator_data(df):

    # Schema Selection
    target_columns = ["bioguideId", "name", "partyName", "state"]
    silver_df = df[target_columns].copy()

    # Business Logic: Filter by Party
    initial_count = len(silver_df)
    silver_df = silver_df[silver_df["partyName"] == "Democratic"]
    final_count = len(silver_df)

    # Standardization: State to lowercase
    silver_df["state"] = silver_df["state"].str.lower()
    logger.info("Filtered %s legislators out of %s", initial_count - final_count, initial_count)
    return silver_df


# ==========================================
# 3. DATA QUALITY VALIDATION
# ==========================================


def validate_silver_data(df):
    """
    Performs multi-layer data validation:
    1. Volume: Minimum record threshold.
    2. Nullability: Enforces MANDATORY vs OPTIONAL fields.
    3. Distribution: Minimum geographic representation (States).
    """
    # --- CHECK 1: Volume Integrity ---
    # We ensure the API didn't return a truncated or empty response.
    if len(df) < config.critical_min_records:
        logger.error(f"Quality check failed: Less than {config.critical_min_records} records in the DataFrame.")
        return False

    # --- CHECK 2: Schema & Nullability (Hard Stop) ---
    # Mandatory columns must be 100% populated for downstream joins.
    # Prevent "Pipeline Breakage" in the Gold layer due to missing IDs or States.
    for col in config.mandatory_columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            logger.error(f"Quality check failed: {null_count} null values in column: {col}")
            return False

    # --- CHECK 3: Data Quality (Soft Warning) ---
    # Optional columns are logged but don't break the pipeline.
    for col in config.optional_columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            logger.warning(f"Quality check failed: {null_count} null values in column: {col}")

    # --- CHECK 4: Geographic Coverage (Business Logic) ---
    # Verifying that the data represents a national scope, not a partial extract.
    unique_states = df["state"].nunique()
    if unique_states < config.expected_min_states:
        logger.error(f"Quality check failed: Less than {config.expected_min_states} unique states in the DataFrame.")
        return False

    # --- CHECK 5: Known Party Names (Homeworks)

    logger.info("Data quality check passed.")
    return True


# ==========================================
# 4. MAIN ORCHESTRATION
# ==========================================


def transform_to_silver(processing_date):
    # Get the list of JSON files in the input directory
    partition_date = f"ingested_at={processing_date}"
    input_dir = os.path.join(config.bronze_path, partition_date)
    try:
        logger.info("Starting data transformation for date: %s", partition_date)

        if not os.path.exists(input_dir):
            logger.warning("Input directory does not exist: %s", input_dir)
            return None

        # Get the list of JSON files in the input directory
        files = [f for f in os.listdir(input_dir) if f.endswith(".json")]
        if not files:
            logger.warning("No JSON files found in %s. Skipping transformation", input_dir)
            return None

        # Sort the list of JSON files in reverse order so the most recent file is first
        files.sort()
        target_file = files[-1]

        # Construct the full path to the JSON file
        input_path = os.path.join(input_dir, target_file)

        logger.info("Processing latest JSON file: %s", target_file)

        # Data Ingestion
        # Load the data from the JSON file
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        # Data Flattening
        # Extract the records from the JSON data
        records = raw_data.get("members", [])
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(records)

        if df.empty:
            logger.warning("No records found in %s. Skipping transformation", input_path)
            return None
        logger.info("Extracted %s records from %s", len(df), input_path)

        silver_df = clean_legislator_data(df)

        # Type Casting
        # Convert the "id" column to an integer
        # silver_df['bioguideId'] = silver_df['bioguideId'].astype(int)
        logger.info("Completed data transformation to silver layer...")

        # Create the full directory path
        full_dir_path = os.path.join(config.silver_path, partition_date)
        os.makedirs(full_dir_path, exist_ok=True)

        # Create the full file path
        file_name = "legislators_refined.parquet"
        full_file_path = os.path.join(full_dir_path, file_name)

        # Data Quality Checks (Validation)------------------------
        if not validate_silver_data(silver_df):
            logger.critical("Pipeline halted: Silver data does not meet quality standards.")
            raise ValueError("Data quality check failed.")
        # --------------------------------------------------------

        # Save the DataFrame to a Parquet file
        silver_df.to_parquet(full_file_path, index=False)
        logger.info("Silver layer data saved successfully: %s", full_file_path)
        return silver_df

    except Exception as e:
        logger.error("An error occurred: %s", str(e))
        raise
