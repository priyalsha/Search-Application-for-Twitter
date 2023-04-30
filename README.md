# 694_2023_Team4

Link to the dataset: https://drive.google.com/drive/folders/1ZkEA2yL5fU4qehOUjU_KCcguzul9nmMf?usp=sharing

Steps to Run Kafka and Cassandra:

Step 1: Start zookeeper (bin/zookeeper-server-start.sh config/zookeeper.properties)

Step 2: Start Kafka (bin/kafka-server-start.sh config/server.properties)

Step 3: Create topic (./bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic cassandratopic --create --partitions 1 --replication-factor 1)

3.1: Deleting a topic (./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic cassandratopic)

3.2: Checking if the topic is deleted (./bin/kafka-topics --zookeeper localhost:2181 --describe --topic cassandratopic)

Step 4: Read what the producer is printing (bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cassandratopic --from-beginning)

Step 5: Start Cassandra

5.1: cassandra -f

5.2: cqlsh

Step 5: run producer.py and consumer.py
![image](https://user-images.githubusercontent.com/35104189/234695152-ad14290d-cb85-46c2-ae2c-8997183fe31c.png)


TO activate the virtual env
source dbms_pro/bin/activate

export FLASK_APP="app.py"
export FLASH_ENV="development"

flask run  (Run the flask app)

export PYTHONDONTWRITEBYTECODE=1 (to Avoid the pychace)

