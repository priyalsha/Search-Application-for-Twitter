from kafka import KafkaProducer
import json
import random
import datetime
from random import randrange
from datetime import timedelta
import time

# create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


    js = json.dumps(data)

    # send a message to the topic
    producer.send('customer_topic', js.encode('utf-8'))

    # block until all messages are sent
    producer.flush()

# close the producer connection
producer.close()
