import json
import time
import numpy as np
import websocket
import threading
import os
import subprocess
from datetime import datetime, timezone

TRADE_URL = "wss://fstream.binance.com/ws/btcusdt@trade"
SAVE_EVERY_SECONDS = 60  # Save every minute
LOCAL_SAVE_DIR = "data"
RCLONE_REMOTE = "gdrive:binance-trades"
RCLONE_CONFIG = "rclone.conf"

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
            subprocess.run([
                "rclone", "--config", RCLONE_CONFIG, "copy", local_path, RCLONE_REMOTE
            ], check=True)
            print(f"[Uploaded] {filename} to Google Drive")
            os.remove(local_path)
        except subprocess.CalledProcessError as e:
            print(f"[Error] Upload failed for {filename}: {e}")

def start_ws():
    ws = websocket.WebSocketApp(TRADE_URL, on_message=on_message)
    ws.run_forever()

if __name__ == "__main__":
    os.makedirs(LOCAL_SAVE_DIR, exist_ok=True)
    threading.Thread(target=save_and_upload, daemon=True).start()
    start_ws()
