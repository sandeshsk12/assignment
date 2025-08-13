import asyncio
import json
import os
import time
import logging
from datetime import datetime

import snowflake.connector
import websockets
from dotenv import load_dotenv

# Get the current working directory
current_directory = os.getcwd()

# Define the log file path in the current directory inside a 'logs' folder
log_file_path = os.path.join(current_directory, "logs", "transfers.log")

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

# Load environment variables from .env file
load_dotenv()

def get_snowflake_connection():
    """
    Establishes a connection to the Snowflake database using credentials from environment variables.
    """
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('WAREHOUSE'),
            database=os.getenv('DATABASE'),
            schema='TOKEN_TRANSFERS_SCHEMA'
        )
        logger.info("Successfully connected to Snowflake.")
        return conn
    except Exception as e:
        logger.error("Failed to connect to Snowflake: %s", e)
        raise

# SQL query for inserting token transfer data
SQL_INSERT = """
INSERT INTO TOKEN_TRANSFERS_TABLE (
    blockchain,
    from_address,
    to_address,
    token_address,
    raw_amount,
    transaction_hash,
    event_index,
    block_timestamp,
    block_number,
    block_hash
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# WebSocket endpoint and subscription settings (can be configured via environment variables)
get_block_key=os.getenv("get_block_API_KEY")
WEBSOCKET_URL = f"wss://go.getblock.io/{get_block_key}"
SUBSCRIPTION_MESSAGE = {
    "jsonrpc": "2.0",
    "method": "eth_subscribe",
    "params": [
        "logs",
        {
            "address":  "0x68749665FF8D2d112Fa859AA293F07A622782F38", #xaut token
            "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" # transfer topic
            ]
        }
    ],
    "id": "getblock.io"
}

async def process_message(message: str, cursor, conn):
    """
    Processes a single WebSocket message and inserts the data into the Snowflake table if valid.

    :param message: The raw JSON message string from the WebSocket.
    :param cursor: The Snowflake DB cursor.
    :param conn: The Snowflake connection.
    """
    counter=0 # only to skip the first response regarding success message

    try:
        response_dict = json.loads(message)
        if 'params' in response_dict and 'result' in response_dict['params']:
            blockchain='ethereum' # Hardcoding it because the API is configured only for ethereum service.
            # Convert current Unix time to UTC timestamp string
            current_time_unix = time.time()
            block_timestamp = datetime.utcfromtimestamp(current_time_unix).strftime('%Y-%m-%d %H:%M:%S') # We currently assume the block_timestamp = current_timestamp, ie when getblock sends the info to us
            # For a more accurate block_timestamp, we can match the block_number in logs table with block_number in blocks table. But we take this shortcut for the sake of time.
            result = response_dict['params']['result']
            token_address = result.get('address')
            topics = result.get('topics', [])

            # Extract addresses from topics (last 40 characters)
            from_address = '0x' + topics[1][-40:]
            to_address = '0x' + topics[2][-40:]

            # Convert hexadecimal string to integer
            raw_amount_hex = result.get('data')

            try:
                raw_amount = int(raw_amount_hex[2:], 16) # remove 0x from string
            except ValueError:
                logger.warning("Failed to convert raw amount: %s", raw_amount_hex)
                return

            # Parse block number and event index
            block_number_hex = result.get('blockNumber')
            try:
                block_number = int(block_number_hex[2:], 16) # remove 0x from string
            except (TypeError, ValueError):
                logger.warning("Failed to convert block number: %s", block_number_hex)
                return

            transaction_hash = result.get('transactionHash')
            block_hash = result.get('blockHash')
            event_index_hex = result.get('logIndex')
            try:
                event_index = int(event_index_hex[2:], 16) # remove 0x from string
            except (TypeError, ValueError):
                logger.warning("Failed to convert event index: %s", event_index_hex)
                return

            # Prepare tuple for insertion
            data = (
                blockchain,
                from_address,
                to_address,
                token_address,
                raw_amount,
                transaction_hash,
                event_index,
                block_timestamp,
                block_number,
                block_hash
            )
            counter = 1
            try:
                cursor.execute(SQL_INSERT, data)
                conn.commit()
                # logger.info("Data inserted successfully: %s", data) -- Turned off because off data storage consideration.
            except Exception as e:
                logger.error("Error inserting data into Snowflake: %s", e)
        else:
            if counter==1:
                logger.warning("Missing expected data in the response: %s", response_dict)
            else:
                try:
                    logger.info("Started ingesting data: %s", datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
                except Exception as e:
                    logger.warning("Failed recording start time: %s", e)

    except json.JSONDecodeError:
        logger.error("Failed to decode JSON message: %s", message)

async def subscribe():
    """
    Subscribes to the WebSocket and processes incoming messages.
    Contains reconnection logic to handle dropped connections.
    """
    while True:
        try:
            async with websockets.connect(WEBSOCKET_URL) as websocket:
                logger.info("Connected to WebSocket at %s", WEBSOCKET_URL)

                # Send the subscription message
                await websocket.send(json.dumps(SUBSCRIPTION_MESSAGE))
                logger.info("Subscription message sent.")

                # Establish connection to Snowflake
                conn = get_snowflake_connection()
                cursor = conn.cursor()

                try:
                    # Continuously process incoming messages
                    while True:
                        message = await websocket.recv()
                        await process_message(message, cursor, conn)
                except websockets.exceptions.ConnectionClosed as e:
                    logger.error("WebSocket connection closed: %s", e)
                except Exception as e:
                    logger.error("Error during message processing: %s", e)
                finally:
                    cursor.close()
                    conn.close()
                    logger.info("Snowflake connection closed.")

        except Exception as e:
            logger.error("WebSocket connection error: %s", e)

        # Wait before trying to reconnect
        logger.info("Reconnecting in 5 seconds...")
        await asyncio.sleep(5)

def main():
    """
    Main entry point. Runs the WebSocket subscription in an asynchronous loop.
    """
    try:
        asyncio.run(subscribe())
    except KeyboardInterrupt:
        logger.info("Program terminated by user.")
    except Exception as e:
        logger.error("Unexpected error: %s", e)

if __name__ == "__main__":
    main()
