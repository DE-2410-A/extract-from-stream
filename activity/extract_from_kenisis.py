import boto3
import pandas as pd
import json

# Initialize a boto3 client for Kinesis
kinesis_client = boto3.client('kinesis', region_name='your-region')

# Function to get records from Kinesis stream
def get_kinesis_records(stream_name, shard_id, shard_iterator_type='LATEST', limit=100):
    # Get the shard iterator
    response = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType=shard_iterator_type
    )
    shard_iterator = response['ShardIterator']

    # Get records from the stream
    response = kinesis_client.get_records(
        ShardIterator=shard_iterator,
        Limit=limit
    )
    return response['Records']

# Stream name and shard ID
stream_name = 'your-stream-name'
shard_id = 'shardId-000000000000'

# Get records from the Kinesis stream
records = get_kinesis_records(stream_name, shard_id)

# Parse the records and load them into a pandas DataFrame
data = []
for record in records:
    data.append(json.loads(record['Data']))

df = pd.DataFrame(data)
print(df)