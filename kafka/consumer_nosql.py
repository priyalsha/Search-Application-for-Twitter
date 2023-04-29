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
consumer = KafkaConsumer('cassandratopic', bootstrap_servers=['localhost:9092'], group_id='3', auto_offset_reset='latest')
print("After connecting to kafka")

tweet_insert_stmt = session.prepare("insert into tweets (tweet_id, tweet_text, tweet_user_id, quote_count, reply_count, retweet_count, favorite_count, tweet_created_at, language) values (?,?,?,?,?,?,?,?,?)")
retweet_insert_stmt = session.prepare("insert into retweets (retweet_id,  retweet_user_id, parent_tweet_id, parent_user_id, retweet_created_at) values (?,?,?,?,?)")
hashtag_insert_stmt = session.prepare("insert into hashtags (hashtag_text, hashtag_created_at, tweet_id) values (?,?,?)")
#test = session.prepare("insert into test (tweet_id, tweet_text) values (?,?)")

def insert(message):
	js = json.loads(message)
	if js['flag'] == 1:
		tweet_id = js['tweet_id']
		tweet_text = js['tweet_text']
		tweet_user_id = js['tweet_user_id']
		tweet_user_id = js['tweet_user_id']
		quote_count = js['quote_count']
		reply_count = js['reply_count']
		retweet_count = js['retweet_count']
		favorite_count = js['favorite_count']
		tweet_created_at = js['tweet_created_at']
		tweet_created_at = datetime.strptime(tweet_created_at, "%Y-%m-%d %H:%M:%S")
		language = js['language']
		retweet_id = js['retweet_id']
		retweet_user_id = js['retweet_user_id']
		parent_tweet_id = js['parent_tweet_id']
		parent_user_id = js['parent_user_id']
		retweet_created_at = js['retweet_created_at']
		retweet_created_at = datetime.strptime(retweet_created_at, "%Y-%m-%d %H:%M:%S")
		hashtag_text = js['hashtag_text']
		hashtag_created_at = js['hashtag_created_at']
		hashtag_created_at = datetime.strptime(hashtag_created_at, "%Y-%m-%d %H:%M:%S")
	else:
		tweet_id = js['tweet_id']
		tweet_text = js['tweet_text']
		tweet_user_id = js['tweet_user_id']
		tweet_user_id = js['tweet_user_id']
		quote_count = js['quote_count']
		reply_count = js['reply_count']
		retweet_count = js['retweet_count']
		favorite_count = js['favorite_count']
		tweet_created_at = js['tweet_created_at']
		tweet_created_at = datetime.strptime(tweet_created_at, "%Y-%m-%d %H:%M:%S")
		language = js['language']
	
	if js['flag'] == 1:
		session.execute(tweet_insert_stmt, (tweet_id, tweet_text, tweet_user_id, quote_count, reply_count, retweet_count, favorite_count, tweet_created_at, language))
		session.execute(retweet_insert_stmt, (retweet_id,  retweet_user_id, parent_tweet_id, parent_user_id, retweet_created_at))
		session.execute(hashtag_insert_stmt, (hashtag_text, hashtag_created_at, tweet_id))
	else:
		session.execute(tweet_insert_stmt, (tweet_id, tweet_text, tweet_user_id, quote_count, reply_count, retweet_count, favorite_count, tweet_created_at, language))


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