from datetime import datetime, timedelta
import mysql.connector
import json
from flask import make_response, jsonify
from config.config import dbconfig
from datetime import datetime, timedelta
import mysql.connector
import json
from flask import make_response, jsonify
from config.config import dbconfig
from collections import OrderedDict
import time


cache = OrderedDict()
MAX_CACHE_SIZE = 1000
DEFAULT_CACHE_TTL = 3600

def get_timestamp():
    return int(datetime.utcnow().timestamp())

class user_model():
    def __init__(self):
        self.con = mysql.connector.connect(host=dbconfig['host'],user=dbconfig['username'],password=dbconfig['password'],database=dbconfig['database'])
        self.con.autocommit=True
        self.cur = self.con.cursor(dictionary=True)
        
    def all_user_model(self):
        start_time = time.time()
        if 'all_user_model' in cache:
            entry = cache['all_user_model']
            print("Retrieving from Cache...")
            if entry['expires_at'] > get_timestamp():
                entry['last_accessed_at'] = get_timestamp()
                result = entry['result']
            else:
                del cache['all_user_model']
                result = None

        else:
            result = None

        end_time = time.time()
        delta = end_time - start_time
        print(f"Time taken to retrieve from Cache is {delta} seconds")

        if result is None:
            print("Retrieving from Database...")
            self.cur.execute("SELECT * FROM users")
            result = self.cur.fetchall()

            end_time = time.time()
            delta = end_time - start_time
            print(f"Time taken to retrieve from Database is {delta} seconds")

            cache['all_user_model'] = {
                'result': result,
                'expires_at': get_timestamp() + DEFAULT_CACHE_TTL,
                'last_accessed_at': get_timestamp()
            }
            if len(cache) > MAX_CACHE_SIZE:
                cache.popitem(last=False)

        if len(result)>0:
            return {"payload":result}
        else:
            return "No Data Found"


    def get_specific_user(self, userid):
        # Check if the result is already cached and not expired
        start_time = time.time()
        if f'specific_user_{userid}' in cache:
            entry = cache[f'specific_user_{userid}']
            if entry['expires_at'] > get_timestamp():
                entry['last_accessed_at'] = get_timestamp()
                result = entry['result']
                end_time = time.time()
                delta = end_time - start_time
                print(f"Time taken to retrieve from Cache is {delta} seconds")
            else:
                del cache[f'specific_user_{userid}']
                result = None
        else:
            result = None

        # If the result is not cached or expired, execute the SQL query
        if result is None:
            user = (userid,)
            self.cur.execute("SELECT * FROM users where name=%s", user)
            result = self.cur.fetchall()

            end_time = time.time()
            delta = end_time - start_time
            print(f"Time taken to retrieve from Database is {delta} seconds")
            
            # Cache the result with TTL expiry and LRU eviction
            cache[f'specific_user_{userid}'] = {
                'result': result,
                'expires_at': get_timestamp() + DEFAULT_CACHE_TTL,
                'last_accessed_at': get_timestamp()
            }
            if len(cache) > MAX_CACHE_SIZE:
                cache.popitem(last=False)

        if len(result) > 0:
            return {"payload": result}
        else:
            return "No Data Found"
        
    def get_user_search(self,name, location):
        user = (name,location,)
        self.cur.execute("SELECT * FROM users where name=%s and location=%s",user)
        result = self.cur.fetchall()
        if len(result)>0:
            return {"payload":result}

        else:
            return "No Data Found"
        

    def wild_search(self, userid):
        userid = '%' + userid + '%'
        user = (userid,)

        self.cur.execute("SELECT * FROM users where name LIKE %s",user)
        result = self.cur.fetchall()
        if len(result)>0:
            return {"payload":result}

        else:
            return "No Data Found"
    
    def user_count(self, userid):
        user = (userid,)

        self.cur.execute("SELECT uc.*, u.name FROM users u, user_count uc where u.user_id = uc.user_id and u.name = %s ", user)
        result = self.cur.fetchall()
        if len(result)>0:
            return {"payload":result}

        else:
            return "No Data Found"
        
    def top10_users(self):

        self.cur.execute("SELECT u.name, uc.followers_count FROM users u, user_count uc where u.user_id = uc.user_id ORDER BY followers_count DESC LIMIT 10")
        result = self.cur.fetchall()
        if len(result)>0:
            return {"payload":result}

        else:
            return "No Data Found"