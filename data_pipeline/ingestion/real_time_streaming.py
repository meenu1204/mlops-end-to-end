# Import necessary libraries
import yfinance as yf
import time
import pandas as pd

# Declare constants
SYMBOL = "AAPL" # Stock ticker
INTERVAL = "15m"
HISTORY_PERIOD = "60d" # Fetch last 365 days of historical data
RECENT_PERIOD = "1d"
STREAMING_INTERVAL = 900 # 15 minutes in seconds

# Fetch historical data
def fetch_historical_data(ticker):
    data = ticker.history(period = HISTORY_PERIOD, interval = INTERVAL)
    return data

# Fetch latest data
def fetch_recent_data(ticker):
    data = ticker.history(period = RECENT_PERIOD, interval = INTERVAL)
    return data.tail(1)

ticker = yf.Ticker(SYMBOL)
historical_data = fetch_historical_data(ticker)
print("Historical data fetched", historical_data.head())

while True:
    recent_data = fetch_recent_data(ticker)
    print("Latest_data fetched", recent_data)
    data = pd.concat([historical_data, recent_data]).drop_duplicates()
    historical_data.to_csv("stock_data.csv", index=True)
    time.sleep(STREAMING_INTERVAL)