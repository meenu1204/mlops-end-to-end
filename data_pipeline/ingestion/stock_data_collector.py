# Name: stock_data_collector.py
# Description: collect historical and recent data for a specific ticker "AAPL" and store it to a .csv file

# Import necessary libraries
# Standard library
import time
from datetime import datetime, time as et_time
from zoneinfo import ZoneInfo

# Third-party library
import yfinance as yf # Fetches stock market data
import yaml
import pandas as pd

from main.logger_config import get_logger

logger = get_logger()

# Load configuration
def load_config(path: str = 'main/config.yaml') -> dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)
    
cfg = load_config()

SYMBOL = cfg['symbol'] # Stock ticker
INTERVAL = cfg['interval'] # Data frequency
HISTORY_PERIOD = cfg['history_period'] # Fetch last 60 days of historical data
RECENT_PERIOD = cfg['recent_period'] # Fetch recent 1 day data
STREAMING_INTERVAL = int(cfg['streaming_interval']) # 1 day in seconds
OUTPUT_FILE = cfg['output_file']

# Ensure fetching of recent data during US market hours
def is_market_open() -> bool:
    eastern = ZoneInfo('US/Eastern')
    now_et = datetime.now(tz=eastern)
    market_open = et_time(9,30)
    market_close = et_time(16,00)
    return now_et.weekday() < 5 and market_open <= now_et.time() <= market_close

# Fetch historical data
def fetch_historical_data(ticker: yf.Ticker) -> pd.DataFrame:
    return ticker.history(period = HISTORY_PERIOD, interval = INTERVAL)

# Fetch latest data
def fetch_recent_data(ticker: yf.Ticker) -> pd.DataFrame:
    return ticker.history(period = RECENT_PERIOD, interval = INTERVAL).tail(1)

def write_to_csv(combined_data: pd.DataFrame) -> None:
    combined_data.to_csv(OUTPUT_FILE, index=True)
    logger.info(f'New data written to {OUTPUT_FILE}')

def collect_data():
    # Create a ticker object
    ticker = yf.Ticker(SYMBOL)

    historical_data = fetch_historical_data(ticker)
    logger.info(f'Historical data fetched for {SYMBOL}, head: {historical_data.head()}')

    while True:
        if is_market_open():
            recent_data = fetch_recent_data(ticker)
            logger.info(f'Latest data fetched: {recent_data}')
    
            # Ensure new data exists
            if not recent_data.empty:
                combined_data = pd.concat([historical_data, recent_data]).drop_duplicates()
                write_to_csv(combined_data)
            else:
                logger.info('No new data fetched')
        else:
            logger.info('Market closed. Skipping data fetch')
        time.sleep(STREAMING_INTERVAL)