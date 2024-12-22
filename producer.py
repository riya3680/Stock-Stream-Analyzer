from confluent_kafka import Producer
from datetime import datetime
import json
import yfinance as yf

# Import Kafka server configuration
from config import kafka_server_config

# Kafka delivery report callback
def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        val = event.value().decode('utf8')
        print(f'{val} sent to partition {event.partition()}.')

# Function to fetch stock data with full details
def fetch_yahoo_stock_data_per_ticker(ticker, limit=500):
    stock_data_list = []
    start_date = f"{datetime.now().year}-01-01"  # Start of the current year
    end_date = datetime.now().strftime("%Y-%m-%d")  # Today's date
    data = yf.download(ticker, start=start_date, end=end_date, interval="1d")  # Daily interval

    if not data.empty:  # Check if data exists
        for index, row in data.iterrows():
            # Set adj_close to None if 'Adj Close' is not in the data
            adj_close = round(float(row['Adj Close']), 2) if 'Adj Close' in data.columns else None

            stock_data = {
                "ticker": ticker,
                "open": round(float(row['Open']), 2),  # Opening price
                "high": round(float(row['High']), 2),  # Highest price
                "low": round(float(row['Low']), 2),  # Lowest price
                "close": round(float(row['Close']), 2),  # Closing price
                "adj_close": adj_close,  # Adjusted close price (None if not available)
                "volume": int(row['Volume']),  # Volume
                "timestamp": index.strftime("%Y-%m-%dT%H:%M:%S"),  # Data timestamp
                "current_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")  # Current timestamp
            }
            stock_data_list.append(stock_data)

            # Stop if the limit is reached
            if len(stock_data_list) >= limit:
                break

    return stock_data_list


if __name__ == '__main__':
    producer = Producer(kafka_server_config)
    tickers = ["AAPL", "GOOG", "AMZN", "MSFT", "TSLA"]  # List of tickers

    # Fetch and send stock data for each ticker
    for ticker in tickers:
        stock_data_list = fetch_yahoo_stock_data_per_ticker(ticker, limit=500)
        for i, stock_data in enumerate(stock_data_list):
            producer.produce(
                "topic-1",  # Your Kafka topic name
                key=f"{ticker}-{i}",  # Use ticker and index as key
                value=json.dumps(stock_data),  # Serialize the stock data as JSON
                on_delivery=callback
            )

    producer.flush()
