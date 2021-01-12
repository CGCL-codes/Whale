# Whale

# 1. Introduction
With the ability to cope with the emerging big streams, Distributed Stream Processing Systems (DSPSs) have attracted much research efforts. 
To achieve high throughput and low latency, DSPSs exploit different parallel processing techniques. 
The one-to-many data partitioning strategy plays an important role in various applications. 
With one-to-many data partitioning, the upstream processing instance sends a generated tuple to a potentially large number of downstream processing instances. 
Existing DSPSs leverages the instance-oriented communication, where an upstream instance transmits a tuple to different downstream instances separately. 
However, in one-to-many data partitioning, the multiple downstream instances typically run on the same machine to exploit multi-core resources. 
Therefore,  a DSPS actually sends a same data item to a machine multiple times, raising significant unnecessary costs for serialization and communications. 
We show that such a mechanism can lead to serious performance bottleneck due to CPU overload. 

To address the problem, we design and implement Whale, an efficient RDMA-assisted distributed stream processing system. 
Two factors contribute to the efficiency of this design. 
First, we propose a novel RDMA-assisted stream multicast scheme with a self-adjusting non-blocking tree structure to alleviate the CPU workloads of an upstream instance during data partitioning. 
Second, we re-design the communication mechanism in existing DSPSs by replacing the instance-oriented communication with a new worker-oriented communication scheme, which saves significant costs for redundant serialization and communications. 
We implement Whale on top of Apache Storm and evaluate it using experiments with large-scale datasets. The results show that Whale achieves 56.6x improvement of system throughput and 97% reduction of processing latency compared to existing designs.

# 2. Documentation
Developers and contributors should also take a look at our [Developer documentation](DEVELOPER.md).

# 3. How to use?
## 3.1 Environment
We deploy the Whale system on a cluster consisting. Each machine is equipped with a 16-core 2.6GHz Intel(R) Xeon(R) CPU , 64GB RAM, 
Red Hat 6.2 system, and a Mellanox InfiniBand FDR 40Gbps NIC and a 1Gbps Ethernet NIC. One machine is configured to be the master node running as nimbus, and the others machines serve as worker nodes running as supervisors.

## 3.2 Building Whale
Developers and contributors shoudl take a look at [Developer documentation](DEVELOPER.md) to build Whale source code.

If you already deploy the Apache Storm (version 2.0.0) cluster environment, you only need to replace these jars to `$STORM_HOME/lib` and `$STORM_HOME/lib-worker`
> * storm-client-2.0.0-SNAPSHOT.jar

Dependent on RDMA jars
> * whale-rdma-2.0.0-SNAPSHOT.jar
> * disni-1.6.jar
> * rdmachannel-core-1.0-SNAPSHOT.jar

### storm-client.jar
Storm-client module source code is maintained using [Maven](http://maven.apache.org/). Generate the excutable jar by running
```
cd storm-client
mvn clean install -Dmaven.test.skip=true -Dcheckstyle.skip=true
```

### whale-rdma.jar
Whale-rdma module source code is maintained using [Maven](http://maven.apache.org/). Generate the excutable jar by running
```
cd whale-rdma
mvn clean install -Dmaven.test.skip=true -Dcheckstyle.skip=true
```

### disni.jar and rdmachannel-core.jar
Whale use RDMA Verbs by [Disni](https://github.com/zrlio/disni) , then we further encapsulate the RDMA Verbs primitive [RDAM-Channel](https://github.com/Whale-Storm/RdmaChannel) to make it more practical and efficient.

Building disni.jar and rdmachannel-core.jar, you just only see [RDMA-Channel](https://github.com/Whale-Storm/RdmaChannel) project. It includes all environments running RDMA-channel.

# 4. Whale Benchmark
## 4.1 Buidling Benchmark
Whale benchmark code is maintained using [Maven](http://maven.apache.org/). Generate the excutable jar by running
```
cd benchmark/benchmark-xxx.jar
mvn clean install -Dmaven.test.skip=true -Dcheckstyle.skip=true
```

## 4.2 Running Benchmark
After deploying a Whale cluster, you can launch Whale by submitting its jar to the cluster. Please refer to Storm documents for how to
[set up a Storm cluster](https://storm.apache.org/documentation/Setting-up-a-Storm-cluster.html) and [run topologies on a Storm cluster](https://storm.apache.org/documentation/Running-topologies-on-a-production-cluster.ht)

``` shell
storm jar benchmark-multicastModel-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelBalancedParitalBenchTopology MulticastBenchTopology ordersTopic 24 1 23 4 100000 rdma 1
storm jar benchmark-multicastModel-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelBinomialTreeBenchTopology MulticastBenchTopology ordersTopic 24 1 23 100000 rdma 1
storm jar benchmark-multicastModel-2A.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelSequentialBenchTopology SequentialMulticastBenchTopology ordersTopic 24 1 23 500000 rdma 1
```

(Didi data and NASDQ data have to be import before running)
## License
Whale is released under the [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).
