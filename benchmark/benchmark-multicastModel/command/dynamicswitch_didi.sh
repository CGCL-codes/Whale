#!/bin/bash
# RDMA Throughput: MulticastModelSequentialBenchTopology
storm jar benchmark-multicastModel-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelSequentialBenchTopology SequentialMulticastBenchTopology ordersTopic 24 1 23 80000 rdma 1
storm jar benchmark-multicastModel-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelSequentialBenchTopology SequentialMulticastBenchTopology ordersTopic 24 1 23 100000 rdma 1

# RDMA Throughput: MulticastModelBalancedParitalBenchTopology
storm jar benchmark-multicastModel-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelBalancedParitalBenchTopology MulticastBenchTopology ordersTopic 24 1 23 100 80000 rdma 1
storm jar benchmark-multicastModel-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelBalancedParitalBenchTopology MulticastBenchTopology ordersTopic 24 1 23 3 100000 rdma 1

# RDMA Latency: MulticastModelSequentialBenchTopology
storm jar benchmark-multicastModel-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelSequentialBenchTopology SequentialMulticastBenchTopology ordersTopic 24 1 23 80000 rdma 1
storm jar benchmark-multicastModel-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelSequentialBenchTopology SequentialMulticastBenchTopology ordersTopic 24 1 23 100000 rdma 1

# RDMA Latency: MulticastModelBalancedParitalBenchTopology
storm jar benchmark-multicastModel-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelBalancedParitalBenchTopology MulticastBenchTopology ordersTopic 24 1 23 100 80000 rdma 1
storm jar benchmark-multicastModel-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelBalancedParitalBenchTopology MulticastBenchTopology ordersTopic 24 1 23 3 100000 rdma 1

# Mysql Command
delete from t_throughput;
delete from t_throughput where throughput < 20000;
select avg(throughput) from t_throughput;
select avg(latency) from t_latency;
