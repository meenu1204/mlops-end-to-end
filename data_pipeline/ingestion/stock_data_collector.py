# Import necessary libraries

import yfinance as yf # Fetches stock market data
import yaml
import time
import logging
import pandas as pd

# logging
logging.basicConfig(
    filename='streaming.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration

# Load from config.yaml
with open("config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

SYMBOL = cfg.get("symbol", "AAPL") # Stock ticker
INTERVAL = cfg.get("interval", "15m") # Data frequency
HISTORY_PERIOD = cfg.get("history_period", "60d") # Fetch last 60 days of historical data
RECENT_PERIOD = cfg.get("recent_period", "1d") # Fetch recent 1 day data
STREAMING_INTERVAL = int(cfg.get("streaming_interval", "86400")) # 1 day in seconds
OUTPUT_FILE = cfg.get("output_file", "stock_market_data.csv")

# Main program

# Fetch historical data
def fetch_historical_data(ticker):
    return ticker.history(period = HISTORY_PERIOD, interval = INTERVAL)
    

# Fetch latest data
def fetch_recent_data(ticker):
    return ticker.history(period = RECENT_PERIOD, interval = INTERVAL).tail(1)

def main():
    # Create a ticker object
    ticker = yf.Ticker(SYMBOL)

    historical_data = fetch_historical_data(ticker)
    logging.info(f"Historical data fetched for {SYMBOL}, head: {historical_data.head()}")

    while True:
        recent_data = fetch_recent_data(ticker)
        logging.info(f"Latest data fetched: {recent_data}")
    
         # Ensure new data exists
        if not recent_data.empty:
            data = pd.concat([historical_data, recent_data]).drop_duplicates()
            data.to_csv(OUTPUT_FILE, index=True)
            logging.info(f"New data written to {OUTPUT_FILE} at {recent_data.index[0]}")
        else:
            logging.warning("No new data fetched")
    
        time.sleep(STREAMING_INTERVAL)

if __name__ == "__main__":
    main()py