from datetime import datetime, timedelta
import mysql.connector
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import json
from flask import make_response, jsonify
from config.config import dbconfig
from collections import OrderedDict
import time


class tweet_model():
    def __init__(self):
        self.cluster = Cluster()
        self.session = self.cluster.connect('test')

        self.cache = OrderedDict()
        self.MAX_CACHE_SIZE = 1000
        self.DEFAULT_CACHE_TTL = 3600
        
    def all_tweets(self):
        query = "SELECT * FROM tweets"
        rows = self.session.execute(query)
        results = []
        for row in rows:
            results.append(dict(row._asdict()))

        if len(results)>0:
            return {"payload":results}

        else:
            return "No Data Found"

    def all_tweets(self):
        query = "SELECT * FROM tweets"
        rows = self.session.execute(query)
        results = []
        for row in rows:
            results.append(dict(row._asdict()))

        if len(results)>0:
            return {"payload":results}

        else:
            return "No Data Found"
        
    
    def hashtag_dates(self, hashtag, start, end):
        start_time = time.time()
        cache_key = f"{hashtag}-{start}-{end}"
        if cache_key in self.cache:
            entry = self.cache[cache_key]
            if entry['expires_at'] > datetime.utcnow():
                entry['last_accessed_at'] = datetime.utcnow()
                result = entry['result']
            else:
                del self.cache[cache_key]
                result = None
        else:
            result = None
        end_time = time.time()
        delta = end_time - start_time
        print(f"Time taken to retrieve from Cache is {delta} seconds")   

        if result is None:
            dates = (hashtag, start, end, )
            query = "select * from hashtags WHERE hashtag_text = %s and hashtag_created_at > %s and hashtag_created_at < %s;"
            rows = self.session.execute(query, dates)
            results = []
            for row in rows:
                results.append(dict(row._asdict()))
                
            end_time = time.time()
            delta = end_time - start_time
            print(f"Time taken to retrieve from Database is {delta} seconds")

            if len(results)>0:
                result = {"payload":results}

                self.cache[cache_key] = {
                    'result': result,
                    'expires_at': datetime.utcnow() + timedelta(seconds=self.DEFAULT_CACHE_TTL),
                    'last_accessed_at': datetime.utcnow()
                }
                if len(self.cache) > self.MAX_CACHE_SIZE:
                    self.cache.popitem(last=False)
            else:
                result = "No Data Found"

        return result
        

    def word_find(self, word):
        start_time = time.time()
        if word in self.cache:
            entry = self.cache[word]
            if entry['expires_at'] > datetime.utcnow():
                entry['last_accessed_at'] = datetime.utcnow()
                result = entry['result']
            else:
                del self.cache[word]
                result = None
        else:
            result = None
        end_time = time.time()
        delta = end_time - start_time
        print(f"Time taken to retrieve from Cache is {delta} seconds")

        if result is None:
            query = "SELECT tweet_id, tweet_text FROM tweets"
            rows = self.session.execute(query)
            results = []
            for row in rows:
                results.append(dict(row._asdict()))

            new_result = []
            for i in results:
                if word in i['tweet_text']:
                    new_result.append({i['tweet_id']:i['tweet_text']})

            end_time = time.time()
            delta = end_time - start_time
            print(f"Time taken to retrieve from Database is {delta} seconds")

            if len(new_result)>0:
                result = {"payload":new_result}

                self.cache[word] = {
                    'result': result,
                    'expires_at': datetime.utcnow() + timedelta(seconds=self.DEFAULT_CACHE_TTL),
                    'last_accessed_at': datetime.utcnow()
                }
                if len(self.cache) > self.MAX_CACHE_SIZE:
                    self.cache.popitem(last=False)
            else:
                result = "No Data Found"

        return result

    
