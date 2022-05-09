import datetime
import random
import json
from faker import Faker
from faker.providers import internet
import boto3


#ENTER STREAM NAME
stream_name = "PipelineStream"


fake = Faker()
fake.add_provider(internet)
client = boto3.client('kinesis')

def data():
    return {
        'payment_time': datetime.datetime.now().isoformat(),
        'customer_id': random.randint(100, 500),
        'payment_amount': round(random.uniform(0.01, 20000.00), 2),
        'ipv4_address': fake.ipv4_private()
            }


def generate_data():
    while True:
        record = data()
        print(record)
        client.put_record(StreamName= stream_name,
                          Data=json.dumps(record),
                          PartitionKey="partitionkey") #OPTIONAL PARTITION KEY


if __name__ == '__main__':
    generate_data()