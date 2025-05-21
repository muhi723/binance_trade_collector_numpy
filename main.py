import json
import time
import numpy as np
import websocket
import threading
import os
from datetime import datetime, timezone
from flask import Flask
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import random
import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("trade_collector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Web Server to keep Render Web Service alive
app = Flask(__name__)

@app.route("/")
def home():
    return "Binance Trade Collector is running."

# Constants
TRADE_URL = "wss://fstream.binance.com/ws/btcusdt@trade"  # Binance Futures trade stream
SAVE_EVERY_SECONDS = 60
LOCAL_SAVE_DIR = "data"
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ['https://www.googleapis.com/auth/drive.file']
RETRY_LIMIT = 5
FOLDER_ID = '160pBqGxFOhcHpiUk_Fyj4bOqE1XyB25j'

buffer = []
buffer_lock = threading.Lock()

# WebSocket Handler
def on_message(ws, message):
    try:
        trade = json.loads(message)
        logger.debug(f"Raw message: {message}")
        timestamp = int(trade.get('T', 0))
        price = float(trade.get('p', 0.0))
        qty_str = trade.get('q', '0.0')  # Raw quantity string for logging
        qty = float(qty_str)  # Convert to float64 for full precision
        logger.debug(f"Raw quantity: {qty_str}, Converted: {qty}")
        market_maker = trade.get('m', None)
        if market_maker is not None:
            market_maker = bool(market_maker)
        with buffer_lock:
            buffer.append([timestamp, price, qty, market_maker])
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        logger.error(f"Error parsing message: {e}: {message}")

# GDrive Upload
def upload_to_gdrive(filename, local_path):
    try:
        with open(SERVICE_ACCOUNT_FILE, "r") as f:
            service_account_info = json.load(f)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES)
        service = build('drive', 'v3', credentials=credentials)
        file_metadata = {'name': filename, 'parents': [FOLDER_ID]}
        media = MediaFileUpload(local_path, mimetype='application/octet-stream')
        retry_count = 0
        while retry_count < RETRY_LIMIT:
            try:
                uploaded_file = service.files().create(
                    body=file_metadata, media_body=media, fields='id').execute()
                logger.info(f"Uploaded {filename} to Google Drive with ID: {uploaded_file.get('id')}")
                return
            except HttpError as e:
                logger.error(f"Upload error for {filename}: {e}")
                retry_count += 1
                time.sleep(random.randint(1, 3))
        logger.error(f"Failed to upload {filename} after {RETRY_LIMIT} retries")
    except Exception as e:
        logger.error(f"Upload init error: {e}")

# Save to Local + Upload Thread
def save_and_upload():
    logger.info("Save + Upload thread started.")
    os.makedirs(LOCAL_SAVE_DIR, exist_ok=True)
    dtype = [
        ('timestamp', 'i8'),  # int64 for timestamp
        ('price', 'f8'),      # float64 for price
        ('quantity', 'f8'),   # float64 for quantity
        ('market_maker', 'O') # object for boolean/None
    ]
    while True:
        time.sleep(SAVE_EVERY_SECONDS)
        with buffer_lock:
            if not buffer:
                continue
            local_copy = buffer[:]
            buffer.clear()
        try:
            # Validate data types
            for trade in local_copy:
                if not (isinstance(trade[0], int) and isinstance(trade[1], float) and 
                        isinstance(trade[2], float) and trade[3] in (True, False, None)):
                    logger.warning(f"Invalid trade data: {trade}")
                    continue
            arr = np.array(local_copy, dtype=dtype)
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            local_path = os.path.join(LOCAL_SAVE_DIR, f"trades_{timestamp}.npy")
            np.save(local_path, arr)
            logger.info(f"Saved {local_path}")
            upload_to_gdrive(filename=f"trades_{timestamp}.npy", local_path=local_path)
            os.remove(local_path)
            logger.info(f"Removed local file {local_path}")
        except Exception as e:
            logger.error(f"Error in save_and_upload: {e}")

# WebSocket Thread
def start_ws():
    logger.info("WebSocket thread started.")
    retry_delay = 5
    while True:
        try:
            ws = websocket.WebSocketApp(TRADE_URL, on_message=on_message)
            ws.run_forever()
        except Exception as e:
            logger.error(f"WebSocket error: {e}, Reconnecting in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)  # Exponential backoff

# Thread Starter
def start_background_tasks():
    logger.info("Starting background threads...")
    threading.Thread(target=save_and_upload, daemon=True).start()
    threading.Thread(target=start_ws, daemon=True).start()

# Main Entrypoint
if __name__ == "__main__":
    start_background_tasks()
    logger.info("Starting Flask app...")
    app.run(host="0.0.0.0", port=10000)
