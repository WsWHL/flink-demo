import json
import random
import string
from datetime import datetime

from kafka import KafkaProducer


def send_msg(topic, msg):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    future = producer.send(topic, msg)
    result = future.get()
    print(result)

if __name__ == '__main__':
    topic = 'flink-test'
    while True:
        msg = {
            'a': ''.join(random.sample(string.ascii_letters + string.digits, 8)),
            'b': random.randint(0, 100),
            'c': random.randint(0, 100),
            'time': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        send_msg(topic, msg)
