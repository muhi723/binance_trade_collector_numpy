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

# === Web Server to keep Render Web Service alive ===
app = Flask(__name__)

@app.route("/")
def home():
    return "Binance Trade Collector is running."

# === Constants ===
TRADE_URL = "wss://fstream.binance.com/ws/btcusdt@trade"
SAVE_EVERY_SECONDS = 60
LOCAL_SAVE_DIR = "data"
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ['https://www.googleapis.com/auth/drive.file']
RETRY_LIMIT = 5  # Limit of retries for uploads

buffer = []

def on_message(ws, message):
    global buffer
    try:
        trade = json.loads(message)

        # Extracting all necessary fields safely
        timestamp = int(trade.get('T', 0))  # Trade time
        symbol = trade.get('s', 'UNKNOWN')  # Symbol
        price = float(trade.get('p', 0.0))  # Price
        qty = float(trade.get('q', 0.0))    # Quantity
        buyer_order_id = int(trade.get('b', -1))  # Buyer order ID
        seller_order_id = int(trade.get('a', -1))  # Seller order ID
        market_maker = trade.get('m', None)  # Market maker flag

        buffer.append([timestamp, symbol, price, qty, buyer_order_id, seller_order_id, market_maker])
    
    except Exception as e:
        print(f"[Error parsing message] {e}")

def upload_to_gdrive(filename, local_path):
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials)

    file_metadata = {'name': filename}
    media = MediaFileUpload(local_path, mimetype='application/octet-stream')

    retry_count = 0
    while retry_count < RETRY_LIMIT:
        try:
            uploaded_file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()

            print(f"[Uploaded] {filename} to Google Drive with ID: {uploaded_file.get('id')}")
            return
        except HttpError as e:
            print(f"[Upload Error] {e}")
            retry_count += 1
            time.sleep(random.randint(1, 3))
    print(f"[Failed to Upload] {filename} after {RETRY_LIMIT} retries")

def save_and_upload():
    global buffer
    while True:
        time.sleep(SAVE_EVERY_SECONDS)
        if not buffer:
            continue
        arr = np.array(buffer)
        buffer = []
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"trades_{timestamp}.npy"
        local_path = os.path.join(LOCAL_SAVE_DIR, filename)
        np.save(local_path, arr)
        print(f"[Saved] {filename}")
        try:
            upload_to_gdrive(filename, local_path)
            os.remove(local_path)
        except Exception as e:
            print(f"[Error in save_and_upload] {e}")

def start_ws():
    while True:
        try:
            ws = websocket.WebSocketApp(TRADE_URL, on_message=on_message)
            ws.run_forever()
        except Exception as e:
            print(f"[WebSocket Error] {e}, Reconnecting in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    os.makedirs(LOCAL_SAVE_DIR, exist_ok=True)
    threading.Thread(target=save_and_upload, daemon=True).start()
    threading.Thread(target=start_ws, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
