from kafka import KafkaProducer
import json
import mysql.connector
from flask import Flask, jsonify
import random
import datetime
from random import randrange
from datetime import timedelta
from datetime import datetime


# create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

users = set()

with open("corona-out-3", "r") as f:
    for line in f:
        try:
            flag = 0

            data = json.loads(line)
            
            ##SQL
            #Users
            user = data["user"]
            user_id = user["id"]
            name = user['name']
            screen_name = user['screen_name']
            user_created_at = datetime.strptime(user['created_at'], '%a %b %d %H:%M:%S %z %Y')
            user_created_at = user_created_at.strftime('%Y-%m-%d %H:%M:%S')
            verified = user['verified']
            location = user['location']

            #User Count
            followers_count = user["followers_count"]
            friends_count = user['friends_count']
            listed_count = user['listed_count']
            favourites_count = user['favourites_count']
            statuses_count = user['statuses_count']
            
            ##NoSQL
            #Tweets
            if "retweeted_status" not in data:
                tweet_id = data['id']
                tweet_text = data['text']   
                tweet_user_id = data['user']['id']
                quote_count = data['quote_count']
                reply_count = data['reply_count']
                retweet_count = data['retweet_count']
                favorite_count = data['favorite_count']
                tweet_created_at = datetime.strptime(data['created_at'], '%a %b %d %H:%M:%S %z %Y')
                tweet_created_at = tweet_created_at.strftime('%Y-%m-%d %H:%M:%S')    
                language = data['lang']
            
            else:
                tweet_id = data['retweeted_status']['id']
                tweet_text = data['retweeted_status']['text']
                tweet_user_id = data['retweeted_status']['user']['id']
                quote_count = data['retweeted_status']['quote_count']
                reply_count = data['retweeted_status']['reply_count']
                retweet_count = data['retweeted_status']['retweet_count']
                favorite_count = data['retweeted_status']['favorite_count']
                tweet_created_at = datetime.strptime(data['retweeted_status']['created_at'], '%a %b %d %H:%M:%S %z %Y')
                tweet_created_at = tweet_created_at.strftime('%Y-%m-%d %H:%M:%S')       
                language = data['retweeted_status']['lang']

                #Retweets
                retweet_id = data['id']
                retweet_user_id = data['user']['id']
                parent_tweet_id = data['retweeted_status']['id']
                parent_user_id = data['retweeted_status']['user']['id']
                retweet_created_at = datetime.strptime(data['created_at'], '%a %b %d %H:%M:%S %z %Y')
                retweet_created_at = retweet_created_at.strftime('%Y-%m-%d %H:%M:%S')

                #Hashtags
                for i in data['retweeted_status']['extended_tweet']['entities']['hashtags']:
                    hashtag_text = i['text']
                    hashtag_created_at = datetime.strptime(data['retweeted_status']['created_at'], '%a %b %d %H:%M:%S %z %Y')
                    hashtag_created_at = hashtag_created_at.strftime('%Y-%m-%d %H:%M:%S')
                    tweet_id = data['retweeted_status']['id']
                
                flag = 1
                    
        except:
            continue

        # creating a JSON object for SQL
        sql_data = {'user_id': user_id, 'name': name, 'screen_name': screen_name, 'user_created_at': user_created_at, 'verified': verified, 'location': location, 'followers_count': followers_count,'friends_count': friends_count,'listed_count': listed_count,'favourites_count': favourites_count,'statuses_count': statuses_count}
        sql_js = json.dumps(sql_data)

        # creating a JSON object for NoSQL
        if flag == 1:
            nosql_data =  {'tweet_id': tweet_id, 'tweet_text': tweet_text, 'tweet_user_id': tweet_user_id, 'quote_count': quote_count, 'reply_count': reply_count, 'retweet_count': retweet_count, 'favorite_count': favorite_count, 'tweet_created_at': tweet_created_at, 'language': language, 'retweet_id': retweet_id, 'retweet_user_id': retweet_user_id, 'parent_tweet_id': parent_tweet_id, 'parent_user_id': parent_user_id, 'retweet_created_at': retweet_created_at, 'hashtag_text': hashtag_text, 'hashtag_created_at': hashtag_created_at, 'flag': 1}
            nosql_js = json.dumps(nosql_data)
        else:
            nosql_data =  {'tweet_id': tweet_id, 'tweet_text': tweet_text, 'tweet_user_id': tweet_user_id, 'quote_count': quote_count, 'reply_count': reply_count, 'retweet_count': retweet_count, 'favorite_count': favorite_count, 'tweet_created_at': tweet_created_at, 'language': language, 'flag': 0}
            nosql_js = json.dumps(nosql_data)

        # send a message to the sqltopic
        producer.send('sqltopic', sql_js.encode('utf-8'))

         # send a message to the nosqltopic
        producer.send('cassandratopic', nosql_js.encode('utf-8'))

        # block until all messages are sent
        producer.flush()

# close the producer connection
producer.close()

