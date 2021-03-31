#!/bin/bash
# DiDiOrderMatchOriginACKBenchTopology
storm jar benchmark-didiOrderMatch-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.didimatch.DiDiOrderMatchOriginACKBenchTopology DiDiOrderMatchTopology ordersTopic 24 1 23 100000 netty 1

# DiDiOrderMatchOriginACKThroughputTopology
storm jar benchmark-didiOrderMatch-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.didimatch.DiDiOrderMatchOriginACKThroughputTopology DiDiOrderMatchTopology ordersTopic 24 1 23 100000 netty 1

# Mysql Command
delete from t_throughput;
delete from t_throughput where throughput < 20000;
select avg(throughput) from t_throughput;
