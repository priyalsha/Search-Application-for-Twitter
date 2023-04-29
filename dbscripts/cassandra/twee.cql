CREATE TABLE test.tweets (
    tweet_id bigint,
    tweet_user_id bigint,
    tweet_created_at timestamp,
    retweet_count int,
    favorite_count int,
    language text,
    quote_count int,
    reply_count int,
    tweet_text text,
    PRIMARY KEY ((tweet_id, tweet_user_id), tweet_created_at, retweet_count)
) WITH CLUSTERING ORDER BY (tweet_created_at DESC, retweet_count ASC);

CREATE TABLE test.hashtags (
    hashtag_text text,
    hashtag_created_at timestamp,
    tweet_id bigint,
    PRIMARY KEY (hashtag_text, hashtag_created_at, tweet_id)
) WITH CLUSTERING ORDER BY (hashtag_created_at ASC, tweet_id ASC);

CREATE TABLE test.retweets (
    retweet_id bigint,
    parent_tweet_id bigint,
    retweet_created_at timestamp,
    parent_user_id bigint,
    retweet_user_id bigint,
    PRIMARY KEY ((retweet_id, parent_tweet_id), retweet_created_at)
) WITH CLUSTERING ORDER BY (retweet_created_at DESC)
