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
RETRY_LIMIT = 5
FOLDER_ID = '160pBqGxFOhcHpiUk_Fyj4bOqE1XyB25j'

buffer = []
buffer_lock = threading.Lock()

def on_message(ws, message):
    global buffer
    try:
        trade = json.loads(message)

        timestamp = int(trade.get('T', 0))
        symbol = trade.get('s', 'UNKNOWN')
        price = float(trade.get('p', 0.0))
        qty = float(trade.get('q', 0.0))
        buyer_order_id = int(trade.get('b', -1))
        seller_order_id = int(trade.get('a', -1))
        market_maker = trade.get('m', None)

        with buffer_lock:
            buffer.append([timestamp, symbol, price, qty, buyer_order_id, seller_order_id, market_maker])
    
    except Exception as e:
        print(f"[Error parsing message] {e}")

def upload_to_gdrive(filename, local_path):
    try:
        with open(SERVICE_ACCOUNT_FILE, "r") as f:
            service_account_info = json.load(f)

        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES)
        service = build('drive', 'v3', credentials=credentials)

        file_metadata = {
            'name': filename,
            'parents': [FOLDER_ID]
        }
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
    except Exception as e:
        print(f"[Upload Init Error] {e}")

def save_and_upload():
    global buffer
    while True:
        time.sleep(SAVE_EVERY_SECONDS)
        with buffer_lock:
            if not buffer:
                continue
            local_copy = buffer[:]
            buffer = []

        try:
            arr = np.array(local_copy)
        except Exception as e:
            print(f"[Array Conversion Error] {e}")
            continue

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
