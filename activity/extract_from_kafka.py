# Import the necessary libraries
from confluent_kafka import Consumer, KafkaException, KafkaError
import pandas as pd

# Configure Kafka Consumer
bootstrap_server = "localhost:9092"
consumer_conf = {
    "bootstrap.servers": bootstrap_server,  # Replace with your Kafka bootstrap servers
    "group.id": "my_group",  # Replace with your consumer group id
    "auto.offset.reset": "earliest",
}

print("Kafka Consumer Configuration:", consumer_conf)

# Create Kafka Consumer
consumer = Consumer(consumer_conf)

# Subscribe to the topic
topic = "random_numbers"  # Replace with your Kafka topic
consumer.subscribe([topic])

# Collect Data from Kafka Stream
messages = []
num_messages_to_collect = 100

try:
    while len(messages) < num_messages_to_collect:
        msg = consumer.poll(timeout=1.0)  # Poll for new messages
        if msg is None:
            print("No message")
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                continue
            else:
                # Error
                raise KafkaException(msg.error())
        else:
            # Message is valid
            messages.append(msg.value().decode("utf-8"))
finally:
    # Close the consumer to commit final offsets
    consumer.close()

# Store the collected values into a pandas DataFrame.

# Create a pandas DataFrame from the collected messages
df = pd.DataFrame(messages, columns=["message"])

# Display the first few and last few rows of the DataFrame
df
