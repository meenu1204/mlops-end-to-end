# File name: stock_data_collector
# Collect historical and recent data for a specific ticker ans store it to a .csv file

# Import necessary libraries

import yfinance as yf # Fetches stock market data
import yaml
import pandas as pd
import logging
import time
from datetime import datetime, time as et_time
from zoneinfo import ZoneInfo


# logging setup
logging.basicConfig(
    filename='streaming.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load configuration
with open("config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

SYMBOL = cfg["symbol"] # Stock ticker
INTERVAL = cfg["interval"] # Data frequency
HISTORY_PERIOD = cfg["history_period"] # Fetch last 60 days of historical data
RECENT_PERIOD = cfg["recent_period"] # Fetch recent 1 day data
STREAMING_INTERVAL = int(cfg["streaming_interval"]) # 1 day in seconds
OUTPUT_FILE = cfg["output_file"]

# Ensure fetching of recent data during US market hours
def is_market_open():
    eastern = ZoneInfo("US/Eastern")
    now_et = datetime.now(tz=eastern)
    market_open = et_time(9,30)
    market_close = et_time(16,00)
    return now_et.weekday() < 5 and market_open <= now_et.time() <= market_close


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
        if is_market_open():
            recent_data = fetch_recent_data(ticker)
            logging.info(f"Latest data fetched: {recent_data}")
    
            # Ensure new data exists
            if not recent_data.empty:
                data = pd.concat([historical_data, recent_data]).drop_duplicates()
                data.to_csv(OUTPUT_FILE, index=True)
                logging.info(f"New data written to {OUTPUT_FILE} at {recent_data.index[0]}")
            else:
                logging.info("No new data fetched")
        else:
            logging.info("Market closed. Skipping data fetch")
        time.sleep(STREAMING_INTERVAL)
# Entry
if __name__ == "__main__":
    main()