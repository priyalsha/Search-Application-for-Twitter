##Code that will access data from cassandra to retrieves the tweet ID and text of the 10 most retweeted tweets as well as implement an LRU cache with a TTL expiry mechanism

from cassandra.cluster import Cluster
from collections import OrderedDict
import time

# Connect to Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('twitter')

# Define cache size and TTL in seconds
CACHE_SIZE = 100
TTL = 60*60  # 1 hour

# Define LRU cache with OrderedDict
cache = OrderedDict()

# Define function to retrieve most retweeted tweets from Cassandra
def get_most_retweeted_tweets():
    query = "SELECT tweet_id, text, retweet_count FROM tweets"
    rows = session.execute(query)

    # Sort rows by retweet_count in descending order
    rows = sorted(rows, key=lambda row: row.retweet_count, reverse=True)

    # Return top 10 most retweeted tweets
    return [(row.tweet_id, row.text) for row in rows[:10]]

# Define function to retrieve tweet from cache or Cassandra
def get_tweet(tweet_id):
    # Check if tweet is in cache
    if tweet_id in cache:
        # Check if tweet is still valid (TTL not expired)
        if time.time() < cache[tweet_id]['timestamp'] + TTL:
            # Update timestamp for LRU cache
            cache.move_to_end(tweet_id)
            return cache[tweet_id]['data']

        # Remove stale tweet from cache
        del cache[tweet_id]

    # Retrieve tweet from Cassandra
    query = "SELECT tweet_id, text FROM tweets WHERE tweet_id=%s"
    row = session.execute(query, [tweet_id]).one()

    # Add tweet to cache (if found in Cassandra)
    if row is not None:
        # Check if cache is full
        if len(cache) >= CACHE_SIZE:
            # Evict least recently used tweet from cache
            cache.popitem(last=False)

        # Add tweet to cache with current timestamp
        cache[tweet_id] = {'data': (row.tweet_id, row.text), 'timestamp': time.time()}

        # Return tweet from Cassandra
        return (row.tweet_id, row.text)

    # Tweet not found in cache or Cassandra
    return None

# Test functions
most_retweeted_tweets = get_most_retweeted_tweets()
print("Most retweeted tweets:")
for tweet_id, text in most_retweeted_tweets:
    print(f"{tweet_id}: {text}")

# Get tweet by ID (first try cache, then try Cassandra)
tweet_id = '123456789'
tweet = get_tweet(tweet_id)
if tweet is not None:
    print(f"Tweet found: {tweet}")
else:
    print(f"Tweet not found: {tweet_id}")
