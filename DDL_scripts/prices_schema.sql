import os
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
import snowflake.connector

# Get the current working directory
current_directory = os.getcwd()

# Define the log file path in the current directory inside a 'logs' folder
log_file_path = os.path.join(current_directory, "logs", "prices.log")

# Ensure the 'logs' directory exists
log_directory = os.path.dirname(log_file_path)
if not os.path.exists(log_directory):
    os.makedirs(log_directory)  # Create the 'logs' directory if it doesn't exist

# Setup logging (only call logging.basicConfig() once)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),  # This outputs logs to the console
        logging.FileHandler(log_file_path)  # This writes logs to a file
    ]
)

logger = logging.getLogger(__name__)

# Load environment variables from a .env file
load_dotenv()

# Configuration dictionary for Snowflake connection
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": "COMPUTE_WH",
    "database": "ASSIGNMENT",
    "schema": "PRICES_SCHEMA"
}

def get_token_price(chain: str, token_address: str, timeout: int = 10) -> dict:
    """
    Fetch the price data for a given token using the CoinGecko API.

    :param chain: The blockchain name (e.g., 'ethereum').
    :param token_address: The token's contract address.
    :param timeout: Request timeout in seconds.
    :return: A dictionary with the price data if successful, else None.
    """
    url = f"https://api.coingecko.com/api/v3/simple/token_price/{chain}"
    params = {
        "contract_addresses": token_address,
        "vs_currencies": "usd",
        "include_last_updated_at": "true"
    }
    headers = {"Accept": "application/json"}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        if not data:
            logger.error("No data received for token %s on chain %s.", token_address, chain)
            return None
        # CoinGecko keys are usually lower case; adjust accordingly.
        token_data = data.get(token_address.lower())
        if token_data is None:
            # Fallback: Use the first returned result if available
            token_data = next(iter(data.values()))
        return token_data
    except Exception as e:
        logger.error("Error fetching price for token %s on %s: %s", token_address, chain, e)
        return None

def insert_token_price(cur, blockchain: str, token_address: str,
                       timestamp: str, usd_price: float, inserted_at: str):
    """
    Insert token price data into the fact_price_table.

    :param cur: The Snowflake cursor.
    :param blockchain: The blockchain name.
    :param token_address: The token's contract address.
    :param timestamp: The timestamp when the token price was last updated. (coingecko)
    :param usd_price: The token price in USD.
    :param inserted_at: The insertion timestamp.
    """
    sql = """
    INSERT INTO fact_price_table
    (blockchain, token_address, timestamp, usd_price, inserted_at)
    VALUES (%s, %s, %s, %s, %s)
    """
    try:
        
        cur.execute(sql, (blockchain, token_address, timestamp, usd_price, inserted_at))
    except Exception as e:
        logger.error("Error inserting token data for %s on %s: %s", token_address, blockchain, e)
        raise
def get_token_list(cur):
    sql = """
    SELECT * from DIM_ASSET_METADATA
    """
    try:
        # Execute the SQL query
        cur.execute(sql)
        
        # Fetch all the results
        results = cur.fetchall()
        return results
    except Exception as e:
        logger.error("Error loading token data: %s", e)
        raise

def main():
    # Define token list by blockchain


    try:
        # Establish a connection using a context manager
        with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
            with conn.cursor() as cur:
                token_list=get_token_list(cur)
                for token_info in token_list:
                    name, symbol, full_name, chain, token, decimals, source = token_info
                    price_data = get_token_price(chain, token)
                    if price_data is None:
                        logger.warning("Skipping token %s on %s due to missing data.", token, chain)
                        continue

                    try:
                        # Ensure the necessary fields are present
                        last_updated = price_data.get("last_updated_at")
                        usd_price = price_data.get("usd")
                        if last_updated is None or usd_price is None:
                            logger.error("Incomplete data for token %s on %s.", token, chain)
                            continue

                        # Convert the Unix timestamp to formatted datetime string
                        timestamp = datetime.fromtimestamp(last_updated).strftime('%Y-%m-%d %H:%M:%S')
                        # Use current time for the record's last_updated_at field
                        inserted_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        # Insert the token price data into the database
                        insert_token_price(cur, chain, token, timestamp, usd_price, inserted_at)
                        logger.info("Data inserted successfully for token %s on %s.", token, chain)
                    except Exception as e:
                        logger.error("Skipping token %s on %s due to processing error: %s", token, chain, e)
                # Commit the transaction once all inserts are processed
                conn.commit()
    except Exception as e:
        logger.error("An error occurred during database operations: %s", e)

if __name__ == "__main__":
    main()
