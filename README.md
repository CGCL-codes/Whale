# Whale

# 1. Introduction
Existing DSPSs implement the one-to-many partition mechanism using a sequence multicast model based on instance-oriented communication,
where an upstream instance transmits tuples to a downstream instance according to the address of the downstream
instance. However, the multiple downstream instances may reside on the same machine. As a result, existing DSPSs need
to send tuples containing the same data item to the same destination machine repeatedly, raising heavy serialization
and inter-server communication overhead. In addition to this, the sequential multicast model causes high data multicast
processing latency. When dealing with highly dynamic data streams, the high multicast processing latency even leads to the block of the stream input.

To address this problem, in this work, we propose Whale, a efficient RDMA-assisted distributed stream processing system. Two factors contribute to the efficiency of the system. 
First, we propose a novel RDMA-assisted stream multicast design. 
Whale monitors the current workloads of the source instance and optimizes the number of cascading instances to form a non-blocking multicast tree to minimize the multicast latency, and thus well copes with highly dynamic data streams. 
Compared to RDMC, Whale ensures that the number of waiting tuples will never exceed the maximum capacity of the transfer queue and makes the processing time of source instance independent of the number of destination instances (i.e., _m_). 
Whale further designs a light-weighted switching mechanism for efficiently adjusting the dynamic multicast structure. 

Second, to alleviate the heavy overhead for serialization and communication, we design and implement a worker-oriented communication design following batch-and-dispatch style. 
Specifically, Whale packages the destination instances IDs with the data item and serializes the _BatchTuple_ only once for multiple destination instances hosted on a same worker. 
The receiving worker then locally dispatches the packet to destination instances according to the IDs. Hence, Whale saves significant computation costs for serialization and communication.

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
