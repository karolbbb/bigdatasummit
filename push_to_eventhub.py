import requests
import json
import time
from azure.eventhub import EventHubProducerClient, EventData
from config import connection_string_primary

# connection_string_primary = 'your_connection_string'

def fetch_and_send_data():
    # Fetch data from CoinDesk API
    url = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population'
    response = requests.get(url)
    data = response.json()

    # Convert data to JSON string
    data_json = json.dumps(data)

    # Send data to Azure Event Hub
    try:
        producer = EventHubProducerClient.from_connection_string(connection_string_primary)
    except Exception as e:
        print(f"Error connecting to primary Event Hub: {e}")

    event_data = EventData(data_json)
    with producer:
        producer.send_batch([event_data])

    print("Data sent to Azure Event Hub successfully.")

while True:
    fetch_and_send_data()
    time.sleep(15)