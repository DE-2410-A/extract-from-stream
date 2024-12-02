import os
import pandas as pd
from google.cloud import pubsub_v1
import json

# Set up Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/your/service-account-file.json"

# Initialize a Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()

# Define the subscription path
project_id = "your-project-id"
subscription_id = "your-subscription-id"
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Function to process messages
def callback(message):
    print(f"Received message: {message.data}")
    data.append(json.loads(message.data))
    message.ack()

# List to store the data
data = []

# Subscribe to the Pub/Sub subscription
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

# Allow the subscriber to run for a while to collect messages
import time
try:
    time.sleep(10)  # Adjust the sleep time as needed to collect enough messages
except KeyboardInterrupt:
    streaming_pull_future.cancel()

# Load the data into a pandas DataFrame
df = pd.DataFrame(data)
print(df)