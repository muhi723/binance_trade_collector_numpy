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

buffer = []

def on_message(ws, message):
    global buffer
    try:
        trade = json.loads(message)
        price = float(trade['p'])
        qty = float(trade['q'])
        ts = int(trade['T'])
        buffer.append([ts, price, qty])
    except Exception as e:
        print(f"[Error parsing message] {e}")

def upload_to_gdrive(filename, local_path):
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials)

    file_metadata = {'name': filename}
    media = MediaFileUpload(local_path, mimetype='application/octet-stream')

    uploaded_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    print(f"[Uploaded] {filename} to Google Drive with ID: {uploaded_file.get('id')}")

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
            print(f"[Upload Error] {e}")

def start_ws():
    ws = websocket.WebSocketApp(TRADE_URL, on_message=on_message)
    ws.run_forever()

if __name__ == "__main__":
    os.makedirs(LOCAL_SAVE_DIR, exist_ok=True)
    threading.Thread(target=save_and_upload, daemon=True).start()
    threading.Thread(target=start_ws, daemon=True).start()
    app.run(host="0.0.0.0", port=10000)
