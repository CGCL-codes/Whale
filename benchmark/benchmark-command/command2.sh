#!/bin/bash
#查看多核CPU利用率 每隔1s钟输出一个结果
sar -P ALL 1 > downcpuinfo.out
sar -P ALL 1 > upcpuinfo.out

#DiDi滴滴打车订单匹配Topology
/whale/kafka/kafka_2.10-0.10.2.0/bin/kafka-topics.sh --create --zookeeper node24:2181,node25:2181,node26:2181 --replication-factor 3 --partitions 1 --topic ordersTopic
cat /whale/DiDiData/orders | /whale/kafka/kafka_2.10-0.10.2.0/bin/kafka-console-producer.sh --broker-list node24:9092,node25:9092,node26:9092,node27:9092,node28:9092,node30:9092 --topic ordersTopic
storm jar /home/zhangfan/didiOrderMatch-2.0.0-SNAPSHOT.jar org.apache.storm.DiDiOrderMatchTopology DiDiOrderMatchTopology ordersTopic_1 3 1 6

#NASDAQ股票交易Topology
/whale/kafka/kafka_2.10-0.10.2.0/bin/kafka-topics.sh --create --zookeeper node24:2181,node25:2181,node26:2181 --replication-factor 3 --partitions 1 --topic stockdealTopic
cat /whale/StockDealData/STOCKtrace3.txt | /whale/kafka/kafka_2.10-0.10.2.0/bin/kafka-console-producer.sh --broker-list node24:9092,node25:9092,node26:9092,node27:9092,node28:9092,node30:9092 --topic stockdealTopic
storm jar NASDAQStockDeal-2.0.0-SNAPSHOT.jar org.apache.storm.StockeDealThroughputTopology StockeDealThroughputTopology stockdealTopic 30 1 30

#数据库操作命令
select avg(latency) from t_latency order by time desc limit 10;
select avg(throughput) from t_throughput

#Kafa操作命令
/opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-topics.sh --create --zookeeper node24:2181,node25:2181,node26:2181 --replication-factor 1 --partitions 6 --topic ordersTopic6
/opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-topics.sh --create --zookeeper node24:2181,node25:2181,node26:2181 --replication-factor 1 --partitions 1 --topic ordersTopic

#写入数据
cat /whale/DiDiData/orders | /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-console-producer.sh --broker-list node24:9092,node25:9092,node26:9092,node27:9092,node28:9092,node30:9092 --topic ordersTopic
cat /whale/StockDealData/STOCKtrace3.txt | /opt/kafka/kafka_2.10-0.10.2.0/bin/kafka-console-producer.sh --broker-list node24:9092,node25:9092,node26:9092,node27:9092,node28:9092,node30:9092 --topic stockdealTopic

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

#测试延迟delay
delete from t_delay where maxdelay>85;
delete from t_delay where maxdelay<0;

#whale github地址
https://github.com/Whale-Storm/whale
13072783289@163.com
woainixxx1314
