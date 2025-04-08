from azure.eventhub import EventHubConsumerClient
import pandas as pd
import json

# Azure Event Hub connection details
connection_str = (
    "Endpoint=sb://<your-event-hub-namespace>.servicebus.windows.net/;"
    "SharedAccessKeyName=<your-policy-name>;"
    "SharedAccessKey=<your-policy-key>"
)
eventhub_name = "<your-event-hub-name>"
consumer_group = "$Default"  # Default consumer group

# List to store the data
data = []


# Callback function to process events
def on_event(partition_context, event):
    print(f"Received event from partition: {partition_context.partition_id}")
    print(f"Event data: {event.body_as_str()}")
    data.append(json.loads(event.body_as_str()))
    partition_context.update_checkpoint(event)


# Create an Event Hub consumer client
client = EventHubConsumerClient.from_connection_string(
    conn_str=connection_str,
    consumer_group=consumer_group,
    eventhub_name=eventhub_name,
)

# Receive events from the Event Hub
try:
    with client:
        client.receive(
            on_event=on_event,
            starting_position="-1",  # Start from the beginning of the stream
        )
except KeyboardInterrupt:
    print("Stopped receiving events.")

# Load the data into a Pandas DataFrame
df = pd.DataFrame(data)
print(df)
