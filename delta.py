import websocket
import json
import pandas as pd
import numpy as np
import time
import os
import logging
from scipy.signal import find_peaks

# Create data directory
os.makedirs('data', exist_ok=True)

# WebSocket URL for Delta Exchange
WEBSOCKET_URL = "wss://socket.india.delta.exchange"

# Store per-symbol data
symbol_dataframes = {}

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket Closed | Status: {close_status_code}, Message: {close_msg}")

def on_open(ws):
    subscribe(ws, "candlestick_1m", ["all"])

def subscribe(ws, channel, symbols):
    payload = {
        "type": "subscribe",
        "payload": {
            "channels": [
                {
                    "name": channel,
                    "symbols": symbols
                }
            ]
        }
    }
    ws.send(json.dumps(payload))

def on_message(ws, message):
    global symbol_dataframes

    message_json = json.loads(message)
    if message_json.get('type') == 'subscriptions':
        return

    df = pd.DataFrame([message_json])
    df = df[~df['symbol'].str.startswith(('P-', 'C-'))]  # Ignore options symbols
    df = df[['symbol', 'timestamp', 'open', 'high', 'low', 'close']]

    if df.empty:
        return

    for symbol in df['symbol'].unique():
        symbol_df = df[df['symbol'] == symbol].copy()
        symbol_df['timestamp'] = symbol_df['timestamp'].apply(
            lambda ts: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts / 1_000_000))
        )

        if symbol not in symbol_dataframes:
            symbol_dataframes[symbol] = symbol_df
        else:
            symbol_dataframes[symbol] = pd.concat([symbol_dataframes[symbol], symbol_df], ignore_index=True)

        # Keep only the latest 100 data points
        symbol_dataframes[symbol] = symbol_dataframes[symbol].tail(1000).reset_index(drop=True)

            # Save to parquet
        file_path = f"data/{symbol}.parquet"
        symbol_dataframes[symbol].to_parquet(file_path, index=False)

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        WEBSOCKET_URL,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()

