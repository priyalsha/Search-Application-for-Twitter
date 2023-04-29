import json
import datetime
from datetime import datetime
import time
from kafka import KafkaConsumer
import mysql.connector

# create a Kafka consumer instance
consumer = KafkaConsumer('sqltopic', bootstrap_servers=['localhost:9092'], group_id='3', auto_offset_reset='latest')
print("After connecting to kafka")

# Connect to the MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="twitter"
)
cursor = mydb.cursor()

def insert(message):
    js = json.loads(message)
    user_id = js['user_id']

    #User
    name = js['name']
    screen_name = js['screen_name']
    user_created_at = js['user_created_at']
    verified = js['verified']
    location = js['location']
    
    #user_count
    followers_count = js['followers_count']
    friends_count = js['friends_count']
    listed_count = js['listed_count']
    favourites_count = js['favourites_count']
    statuses_count = js['statuses_count']

    sql_check = "SELECT * FROM users WHERE user_id = %s"
    val_check = (user_id,)
    cursor.execute(sql_check, val_check)
    result = cursor.fetchone()

    if result:
        print("User_id already exists in the database")
    
    else:
        sql = "INSERT INTO users (user_id, name, screen_name, user_created_at, verified, location) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (user_id, name, screen_name, user_created_at, verified, location)
        cursor.execute(sql, val)
        mydb.commit()

        sql = "INSERT INTO user_count (user_id, followers_count, friends_count, listed_count, favourites_count, statuses_count) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (user_id, followers_count, friends_count, listed_count, favourites_count, statuses_count)
        cursor.execute(sql, val)
        mydb.commit()


# # continuously poll for new messages
while True:
	records = consumer.poll(timeout_ms = 5000)
	if len(records) == 0:
		print("No new messages received. Exiting...")
		break
	for record in records.values():
		for message in record:
			insert (message.value.decode('utf-8'))

consumer.commit()