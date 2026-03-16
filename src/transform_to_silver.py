import pandas as pd
import json
import os
import logging
from config import BRONZE_PATH
from config import SILVER_PATH

logger = logging.getLogger(__name__)


def transform_to_silver(processing_date):
    # Get the list of JSON files in the input directory
    partition_date = f"ingested_at={processing_date}"
    input_dir = os.path.join(BRONZE_PATH, partition_date)
    try: 
        logger.info("Starting data transformation for date: %s", partition_date)

        if not os.path.exists(input_dir):
            logger.warning("Input directory does not exist: %s", input_dir)
            return None

        # Get the list of JSON files in the input directory 
        files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
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
        with open(input_path, 'r', encoding='utf-8') as f:
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

        # Schema Enforcement (Data Selection)
        # Select the target columns from the DataFrame
        target_columns = ['bioguideId', 'name', 'partyName', 'state']
        # Copy the selected columns to a new DataFrame
        silver_df = df[target_columns].copy()

        #print(silver_df.head())

        # Business Logic Filtering
        # Filter the DataFrame to include only Republican legislators
        initial_count = len(silver_df)
        silver_df = silver_df[silver_df["partyName"] == "Democratic"]
        final_count = len(silver_df)
        logger.info("Filtered %s legislators out of %s", initial_count - final_count, initial_count)
        
        # Convert the "state" column to lowercase
        silver_df['state'] = silver_df['state'].str.lower()

        # Type Casting
        # Convert the "id" column to an integer
        #silver_df['bioguideId'] = silver_df['bioguideId'].astype(int)
        logger.info("Completed data transformation to silver layer...")

        full_dir_path = os.path.join(SILVER_PATH, partition_date)
        os.makedirs(full_dir_path, exist_ok=True)

        file_name = "legislators_refined.parquet"
        full_file_path = os.path.join(full_dir_path, file_name)

        silver_df.to_parquet(full_file_path, index=False)
        logger.info("Silver layer data saved successfully: %s", full_file_path)
        return silver_df
    
    except Exception as e:
        logger.error("An error occurred: %s", str(e))
        raise
