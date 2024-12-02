import pika
import pandas as pd
import json

# RabbitMQ connection parameters
rabbitmq_host = 'localhost'
rabbitmq_queue = 'your_queue_name'

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()

# Declare the queue (in case it doesn't exist)
channel.queue_declare(queue=rabbitmq_queue)

# List to store the data
data = []

# Callback function to process messages
def callback(ch, method, properties, body):
    print(f"Received message: {body}")
    data.append(json.loads(body))
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Consume messages from the queue
channel.basic_consume(queue=rabbitmq_queue, on_message_callback=callback, auto_ack=False)

print(f"Listening for messages on {rabbitmq_queue}...\n")

# Start consuming
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

# Close the connection
connection.close()

# Load the data into a pandas DataFrame
df = pd.DataFrame(data)
print(df)