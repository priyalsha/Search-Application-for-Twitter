##Code that will access data from mysql to retrieves the user ID and name of the 10 most followed users as well as implement an LRU cache with a TTL expiry mechanism

import mysql.connector
import time

# Connect to MySQL database
db = mysql.connector.connect(
  host="localhost",
  user="username",
  password="password",
  database="twitter"
)

# Define cache dictionary with maximum size of 1000 entries
cache = {}
max_cache_size = 1000

# Define cache expiry time (in seconds)
cache_expiry_time = 300  # 5 minutes

# Define query to retrieve user ID and name of the 10 most followed users
query = "SELECT user_id, name FROM users ORDER BY followers DESC LIMIT 10"

def get_most_followed_users():
    # Check if data is present in cache
    if 'most_followed_users' in cache:
        # Check if cache entry is still valid
        if time.time() - cache['most_followed_users']['timestamp'] < cache_expiry_time:
            print("Retrieving data from cache")
            return cache['most_followed_users']['data']
        else:
            # Cache entry has expired, remove it from cache
            del cache['most_followed_users']
    
    # Cache miss, retrieve data from database
    print("Retrieving data from database")
    cursor = db.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    
    # Add data to cache
    if len(cache) >= max_cache_size:
        # Cache is full, remove least recently used entry
        oldest_key = min(cache, key=cache.get)
        del cache[oldest_key]
    cache['most_followed_users'] = {'timestamp': time.time(), 'data': data}
    
    return data

# Test the function
print(get_most_followed_users())
