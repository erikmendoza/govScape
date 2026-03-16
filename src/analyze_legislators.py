import pandas as pd 
import os
import logging
from config import SILVER_PATH
from config import GOLD_PATH

logger = logging.getLogger(__name__)


def generate_gold_metrics(processing_date):

    # Get the list of JSON files in the input directory
    partition_date = f"ingested_at={processing_date}"
    input_file = "legislators_refined.parquet"
    input_dir = os.path.join(SILVER_PATH, partition_date, input_file)

    # Check if the file exists 
    if not os.path.exists(input_dir):
        logger.warning("File does not exist: %s", input_dir)
        return None
    
    # Read the parquet file 
    df = pd.read_parquet(input_dir)

    # Check if the dataframe is empty 
    if df.empty:
        logger.warning("Dataframe is empty: %s", input_dir)
        return None
    logger.info("Generating gold metrics for date: %s", partition_date)    

    # Get the top 5 states with most legislators
    top_state = df['state'].value_counts().head(5)
    
    # Create the gold directory if it doesn't exist and rename the file
    os.makedirs(GOLD_PATH, exist_ok=True)
    report_name = f"summary_{processing_date}.csv"

    # Save the top 5 states to a CSV file
    top_state.to_csv(os.path.join(GOLD_PATH, report_name), index=True)
    logger.info("Generated gold metrics for date: %s", partition_date)
    return top_state
