#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install mysql-connector-python')


# In[ ]:


import mysql.connector

mydb = mysql.connector.connect(
  host="yourhostname",
  user="yourusername",
  password="yourpassword",
  database="yourdatabasename"
)


# In[ ]:


mycursor.execute("CREATE TABLE tweets (id BIGINT PRIMARY KEY, tweet_text TEXT, user_id BIGINT, quote_count INT, reply_count INT, retweet_count INT, favorite_count INT, created_at DATETIME, language TEXT)")


# In[ ]:


mycursor.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name text, screen_name text, created_at DATETIME, followers_count INT, location text)")


# In[ ]:


mycursor.execute("CREATE TABLE retweets (retweet_id BIGINT PRIMARY KEY, user_id BIGINT, parent_tweet_id BIGINT, parent_user_id BIGINT, created_at DATETIME)")


# In[ ]:


mycursor.execute("SHOW TABLES")
for table in mycursor:
    print(table)


# In[ ]:





# In[ ]:





# In[ ]:




