import requests
import json
import time
from azure.eventhub import EventHubProducerClient, EventData

connection_string_primary = 'Endpoint=sb://azeventhubnsn6rjrc.servicebus.windows.net/;SharedAccessKeyName=event-hub-policy-kb;SharedAccessKey=VpejHpULoxv8N9nnrUpTaAIXEIaXnKNGI+AEhAv9wSQ=;EntityPath=azeventhubn6rjrc'

def fetch_and_send_data():
    # Fetch data from CoinDesk API
    url = 'https://api.coindesk.com/v1/bpi/currentprice.json'
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