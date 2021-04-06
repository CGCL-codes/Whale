#!/bin/bash
# StockDealMatchOriginACKBenchTopology
storm jar benchmark-stockDealMatch-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.stockdeal.StockDealOriginACKBenchTopology stockDealTopology stockdealTopic 24 1 23 100000 netty 1

# Mysql Command
delete from t_throughput;
delete from t_throughput where throughput < 20000;
select avg(throughput) from t_throughput;
