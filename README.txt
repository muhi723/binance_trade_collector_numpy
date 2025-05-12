# Binance BTCUSDT Trade Collector (NumPy + Google Drive)

## Features:
- Collects every Binance BTCUSDT Futures trade 24/7
- Stores them in NumPy `.npy` files (every 60 seconds)
- Uploads each file to Google Drive using rclone

## Setup:
1. Install rclone and configure `gdrive` remote.
2. Deploy on Render.com (use free background worker):
   - Add this repo
   - Set `Start Command`: `python main.py`
3. Make sure rclone is installed and added to PATH.

Data will appear in your Google Drive under `binance-trades/`