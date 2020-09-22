#!/bin/bash
#Kafa操作命令
/opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-topics.sh --create --zookeeper node24:2181,node25:2181,node26:2181 --replication-factor 1 --partitions 6 --topic ordersTopic6
/opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-topics.sh --create --zookeeper node24:2181,node25:2181,node26:2181 --replication-factor 1 --partitions 1 --topic ordersTopic

/opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-topics.sh --create --zookeeper node24:2181,node25:2181,node26:2181 --replication-factor 1 --partitions 6 --topic stockdealTopic6
/opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-topics.sh --create --zookeeper node24:2181,node25:2181,node26:2181 --replication-factor 1 --partitions 1 --topic stockdealTopic

#写入数据
cat /whale/DiDiData/orders | /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-console-producer.sh --broker-list node24:9092,node25:9092,node26:9092,node27:9092,node28:9092,node30:9092 --topic ordersTopic
cat /whale/DiDiData/orders | /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-console-producer.sh --broker-list node24:9092,node25:9092,node26:9092,node27:9092,node28:9092,node30:9092 --topic ordersTopic6
cat /whale/StockDealData/STOCKtrace3.txt | /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-console-producer.sh --broker-list node24:9092,node25:9092,node26:9092,node27:9092,node28:9092,node30:9092 --topic stockdealTopic
cat /whale/StockDealData/STOCKtrace3.txt | /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-console-producer.sh --broker-list node24:9092,node25:9092,node26:9092,node27:9092,node28:9092,node30:9092 --topic stockdealTopic6
