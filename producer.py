from kafka import KafkaProducer
import json
import random
import datetime
from random import randrange
from datetime import timedelta
import time

# create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for i in range(10000):
    names = [['shubham','kokane'],['bhushan','gupta'],['rutvik','deshpande'],['priyal','shaha'],['rutu','desai'],['atharva','adbe'],['amit','reddy'],['tom','levy'],['anchala','krishnan'],['zhe','zhang']]

    # creating random emailids
    x = random.randint(0,9)
    email_id = names[x][0] +'.' +names[x][1]+'@rutgers.edu'
    
    # creating random login_time
    start = datetime.datetime.strptime("3/22/2022 12:00 AM", '%m/%d/%Y %I:%M %p')
    end = datetime.datetime.strptime("3/21/2023 11:59 PM", '%m/%d/%Y %I:%M %p')
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    login_time = start + timedelta(seconds=random_second)

    # creating random item_bought (True/False)
    item_bought = random.choice([False, True])

    # creating random page_viewed
    page_viewed = random.randint(1,25)

    # creating a JSON object
    data = {'email_id': email_id, 'login_time': login_time.strftime('%Y-%m-%d %H:%M:%S'), 'item_bought': item_bought, 'page_viewed': page_viewed}
    js = json.dumps(data)

    # send a message to the topic
    producer.send('customer_topic', js.encode('utf-8'))

    # block until all messages are sent
    producer.flush()

# close the producer connection
producer.close()
