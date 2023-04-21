from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import json
import datetime
from datetime import datetime
import time
from kafka import KafkaConsumer

cluster = Cluster()
session = cluster.connect('test')

# create a Kafka consumer instance
consumer = KafkaConsumer('customer_topic', bootstrap_servers=['localhost:9092'], group_id='3', auto_offset_reset='latest')
print("After connecting to kafka")

# Measure runtime for inserting records
start_time = time.time()

temp = 0

def insert(message):
    js = json.loads(message)
    email_id = js['email_id']
    month = datetime.strptime(js['login_time'], '%Y-%m-%d %H:%M:%S').month
    login_time = datetime.strptime(js['login_time'], '%Y-%m-%d %H:%M:%S').time()
    page_viewed = js['page_viewed']
    item_bought = js['item_bought']

    delta = 1

    session.execute("update user_views set count += %s where email_id = %s and login_time = %s", (delta, email_id, login_time))
    session.execute("update view_count set count += %s where page_viewed = %s and month = %s", (delta, page_viewed, month))
    session.execute(session.prepare("insert into customer (email_id, month, login_time, page_viewed, item_bought) values (?,?,?,?,?)"), [email_id, month, login_time, page_viewed, item_bought])

# # continuously poll for new messages
while True:
	records = consumer.poll(timeout_ms = 5000)
	if len(records) == 0:
		print("No new messages received. Exiting...")
		break
	for record in records.values():
		for message in record:
			temp += 1
			insert (message.value.decode('utf-8'))

end_time = time.time()
delta = end_time - start_time - 5
print(f"Time taken to insert {temp} records is {delta} seconds")
consumer.commit()
