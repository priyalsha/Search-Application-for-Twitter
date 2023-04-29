CREATE TABLE users (
  user_id BIGINT PRIMARY KEY,
  name text,
  screen_name text,
  user_created_at DATETIME,
  verified boolean,
  location text
);



CREATE TABLE user_count (
  user_id BIGINT PRIMARY KEY,
  followers_count INT,
  friends_count INT,
  listed_count INT,
  favourites_count INT,
  statuses_count INT
);
