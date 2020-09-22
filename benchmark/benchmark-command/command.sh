#!/bin/bash
#查看多核CPU利用率 每隔1s钟输出一个结果
sar -P ALL 1

#DiDi滴滴打车订单匹配Topology
/storm/kafka_2.10-0.10.2.1/bin/kafka-topics.sh --create --zookeeper node100:2181,node101:2181,node102:2181 --replication-factor 3 --partitions 1 --topic ordersTopic_1
cat /storm/DiDiData/orders | /storm/kafka_2.10-0.10.2.1/bin/kafka-console-producer.sh --broker-list node101:9092,node102:9092,node103:9092,node104:9092,node105:9092,node106:9092 --topic ordersTopic_1
storm jar /home/zhangfan/didiOrderMatch-2.0.0-SNAPSHOT.jar org.apache.storm.DiDiOrderMatchTopology DiDiOrderMatchTopology ordersTopic_1 3 1 6

#NASDAQ股票交易Topology
/storm/kafka_2.10-0.10.2.1/bin/kafka-topics.sh --create --zookeeper node100:2181,node101:2181,node102:2181 --replication-factor 3 --partitions 1 --topic stockdealTopic
cat /storm/StockDealData/STOCKtrace3.txt | /storm/kafka_2.10-0.10.2.1/bin/kafka-console-producer.sh --broker-list node101:9092,node102:9092,node103:9092,node104:9092,node105:9092,node106:9092 --topic stockdealTopic
storm jar NASDAQStockDeal-2.0.0-SNAPSHOT.jar org.apache.storm.StockeDealThroughputTopology StockeDealThroughputTopology stockdealTopic 30 1 30

#数据库操作命令
select avg(latency) from t_latency order by time desc limit 10;
select avg(throughput) from t_throughput

#Kafa操作命令
/storm/kafka_2.10-0.10.2.1/bin/kafka-topics.sh --create --zookeeper node100:2181,node101:2181,node102:2181 --replication-factor 3 --partitions 1 --topic ordersTopic_1
cat /storm/DiDiData/orders | /storm/kafka_2.10-0.10.2.1/bin/kafka-console-producer.sh --broker-list node101:9092,node102:9092,node103:9092,node104:9092,node105:9092,node106:9092 --topic ordersTopic_1

storm jar /home/zhangfan/didiOrderMatch-2.0.0-SNAPSHOT.jar org.apache.storm.DiDiOrderMatchTopology DiDiOrderMatchTopology ordersTopic_1 30 1 30

#test latency
mysql -h node100 -u root -p
password: 123456

#数据库操作命令
use whale;
select * from t_latency;
select avg(latency) from t_latency;
select * from t_latency order by time desc limit 10;
select avg(latency) from (select * from t_latency order by time desc limit 10) as letency;
delete from  t_latency;

throughput
select * from t_throughput;
select avg(throughput) from t_throughput;
select * from t_throughput order by time desc limit 10;
select avg(throughput) from (select * from t_throughput order by time desc limit 10) as throughput;
delete from  t_throughput;

storm jar /home/zhangfan/didiOrderMatchWhale-2.0.0-SNAPSHOT.jar org.apache.storm.DiDiOrderMatchLatencyTopology DiDiOrderMatchLatencyTopology ordersTopic_2 30 1 30 1

storm jar /home/zhangfan/didiOrderMatchWhale-2.0.0-SNAPSHOT.jar org.apache.storm.DiDiOrderMatchLatencyTopology DiDiOrderMatchLatencyTopology ordersTopic_1 30 1 30 1

#测试纳斯达克数据
storm jar /home/zhangfan/NASDAQStockDeal-2.0.0-SNAPSHOT.jar org.apache.storm.StockeDealThroughputTopology StockeDealThroughputTopology stockdealTopic 30 1 30

测试吞吐量
java -cp /home/TJ/serializeBenchMark-2.0.0-SNAPSHOT.jar org.apache.storm.StormSerializeBenchmark didi 1

java -cp /home/TJ/serializeBenchMark-2.0.0-SNAPSHOT.jar org.apache.storm.StormSerializeBenchmark nasdaq 1

#whale github地址
https://github.com/Whale-Storm/whale
13072783289@163.com
woainixxx1314
