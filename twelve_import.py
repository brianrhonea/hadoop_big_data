import requests
import json
import socket
import time
from datetime import datetime

# Replace with your Twelve Data API key
API_KEY = "3953f49cbf664ed386a2176ac1c91d78"

# Function to fetch stock data from Twelve Data API
def fetch_stock_data(symbol):
    url = "https://api.twelvedata.com/time_series?symbol={}&interval=1min&apikey={}".format(symbol, API_KEY)
    response = requests.get(url)
    
    # Check if the response is successful
    if response.status_code == 200:
        try:
            data = response.json()
            if 'values' in data:
                return data
            else:
                print("Error in response:", data)
                return {}
        except ValueError as e:
            print("JSON Decode Error:", e)
            return {}
    else:
        print("Error fetching data for {}: Status Code: {}, Response Content: {}".format(symbol, response.status_code, response.text))
        return {}

# Define the stock symbols to monitor
symbols = ["AAPL", "IBM", "GOOGL", "MSFT", "TSLA"]

# Define the localhost server settings
HOST = 'localhost'
PORT = 12345  # You can choose any available port

# Function to send data to localhost server
def send_data_to_localhost(data):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    s.sendall(data + "\n")  # Send data with a newline character
    s.close()

# Function to determine if the market is open (9:30 am to 4:00 pm ET)
def is_market_open():
    return True
    #now = datetime.now()
    #market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    #market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    
    # Checking if it's a weekday (Mon-Fri)
    #if now.weekday() < 5:
        #return market_open <= now <= market_close
    #return False

# Main loop to fetch stock data and send to localhost
def main():
    # Check if it's a business day and market is open
    while is_market_open():
        # Fetch data for all symbols
        for symbol in symbols:
            stock_data = fetch_stock_data(symbol)
            
            # Extract and format data for the console
            if "values" in stock_data:
                latest_data = stock_data["values"][0]  # Get the latest data
                latest_time = latest_data["datetime"]
                print("Symbol: {}, Time: {}, Data: {}".format(symbol, latest_time, latest_data))
                
                # Send the data to localhost server
                formatted_data = json.dumps({symbol: {latest_time: latest_data}})
                send_data_to_localhost(formatted_data)
            
            else:
                print("Error fetching data for {}.".format(symbol))
        
        # Wait 1 minute before the next iteration
        time.sleep(61)

if __name__ == "__main__":
    main()

