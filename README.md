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

To address the problem, we propose Whale, a novel efficient RDMA-assisted one-to-many stream data partitioning design. Whale designs a novel self-adjusting non-blocking tree structure, which prevents the transfer queue of an instance from blocking due to stream dynamics. In addition, Whale designs and implements a new worker-oriented communication mechanism which replaces the instance-oriented design in previous DSPSs and significantly reduces the CPU overhead for serialization and communications. We implement Whale atop Apache Storm and evaluate its performance using large-scale datasets from real-world systems. The results show that Whale achieves 56.6x improvement of system throughput and 97% reduction of processing latency compared to existing designs.

# 2. Architecture of Whale
![image](https://github.com/Whale2021/Whale/blob/master/images/Whale_architecture.png)

Whale includes two main components: stream multicast and worker-oriented communication. The former fully exploits the zero-copy RDMA network to efficiently process highly dynamic streams. 
In the stream multicast component, Whale constructs the proposed efficient non-blocking multicast tree structure to support efficient one-to-many data partitioning. The system can dynamically adjust the structure to copy with stream dynamics. Specially, the stream multicast scheduler elaborately chooses the maximum out-degree of the non-blocking multicast tree structure for the incoming stream and minimizes the average multicast latency. The system workload monitor periodically monitors the current workloads of the source instance while the multicast controller adjusts the multicast structure accordingly. The worker-oriented communication component addresses at reducing the redundant serialization and communication. The batch component packages the IDs with the data item into a BatchTuple and serializes the BatchTuple for multicasting. The dispatcher deserializes the BatchTuple and dispatches the receiving tuple to the locally hosted destination instances according to the obtained instance IDs.

# 3. How to use?
## 3.1 Environment
We deploy the Whale system on a cluster consisting of 30 machines. Each machine is equipped with a 16-core 2.6GHz Intel(R) Xeon(R) CPU , 64GB RAM, 256GB HDD, Red Hat 6.2 system, a Mellanox InfiniBand FDR 56Gbps NIC and a 1Gbps Ethernet NIC. One machine is configured to be the master node running as nimbus, and the others machines serve as worker nodes running as supervisors.

## 3.2 Building Whale
Before building Whale, developers should first build the source code of Apache Storm version 2.0.0-SNAPSHOT and install the library in local warehouse.

If you already deploy Apache Storm (version 2.0.0-SNAPSHOT), you only need to replace these jars to `$STORM_HOME/lib` and `$STORM_HOME/lib-worker`
> * storm-client-2.0.0-SNAPSHOT.jar
> * whale-rdma-2.0.0-SNAPSHOT.jar

Dependent on RDMA jars
> * disni-1.6.jar
> * rdmachannel-core-1.0-SNAPSHOT.jar

### storm-client.jar
The source code of Storm-client module is maintained using [Maven](http://maven.apache.org/) (version 3.5.4). Generate the executable jar by running
```
$ cd storm-client
$ mvn clean install -Dmaven.test.skip=true -Dcheckstyle.skip=true
```

### whale-rdma.jar
The source code of Whale-rdma module is maintained using [Maven](http://maven.apache.org/). Before build the whale-rdma module, you need to generate whale-multicast and whale-common. Generate the executable jar by running
```
$ cd whale-multicast
$ mvn clean install -Dmaven.test.skip=true -Dcheckstyle.skip=true
$ cd ../whale-common
$ mvn clean install -Dmaven.test.skip=true -Dcheckstyle.skip=true
```
After you install the whale-multicast.jar and whale-common.jar, you can generate the executable whale-rdma.jar by running
```
$ cd whale-rdma
$ mvn clean install -Dmaven.test.skip=true -Dcheckstyle.skip=true
```

### disni.jar and rdmachannel-core.jar
Whale use RDMA Verbs by [Disni](https://github.com/zrlio/disni) , then we further encapsulate the RDMA Verbs primitive [RDAM-Channel](https://github.com/Whale2021/RDMAChannel) to make it more practical and efficient.

Building disni.jar and rdmachannel-core.jar, you just only see [RDMA-Channel](https://github.com/Whale2021/RDMAChannel) project. It includes all environments running RDMA-channel.

# 4. Whale Benchmark
## 4.1 Buidling Benchmark
Whale benchmark code is maintained using [Maven](http://maven.apache.org/). Before generate the executable jar, you should build and install the benchmark-common model. Do all these steps by running
```
$ cd benchmark/benchmark-common
$ mvn clean install -Dmaven.test.skip=true -Dcheckstyle.skip=true
$ cd ../benchmark-didiOrderMatch
$ mvn clean package -Dmaven.test.skip=true -Dcheckstyle.skip=true
```
Then you get the didi benchmark benchmark-didiOrderMatch.jar.

Generate the executable jar of benchmark-stockDeal by running 
```
$ cd benchmark/benchmark-stockDeal
$ mvn clean package -Dmaven.test.skip=true -Dcheckstyle.skip=true
```

Then you get the stock benchmark benchmark-stockDeal.jar.

## 4.2 Running Benchmark
After deploying a Whale cluster, you can launch Whale by submitting its jar to the cluster. Please refer to Storm documents for how to
[set up a Storm cluster](https://storm.apache.org/documentation/Setting-up-a-Storm-cluster.html) and [run topologies on a Storm cluster](https://storm.apache.org/documentation/Running-topologies-on-a-production-cluster.html)

Before you run benchmarks, you need to do the follow steps:
* [Set up Zookeeper (version 3.4.6 or higher) cluster](https://zookeeper.apache.org/doc/r3.4.6/index.html)
* [Set up Kafka (version 2.10-0.10.2.0) cluster](https://kafka.apache.org/0102/documentation.html)
* Set up Whale cluster like setting up [Storm](https://storm.apache.org/documentation/Setting-up-a-Storm-cluster.html)
* Create a topic in Kafka and upload the data used for evaluation
```
$ cd KAFKA_HOME
$ bin/kafka-topics.sh --create --topic <ordersTopic> --replication-factor 2 --partitions <numofParitions> --zookeeper <nodexx:2181>
$ cat DATA_HOME/didiData/xxx | bin/kafka-console-producer.sh  --broker-list <nodexx:9092> --sync --topic ordersTopic
$ bin/kafka-topics.sh --create --topic <stockTopic> --replication-factor 2 --partitions <numofPartitions> --zookeeper <nodexx:2181>
$ cat DATA_HOME/stockData/xxx | bin/kafka-console-producer.sh  --broker-list <nodexx:9092> --sync --topic stockTopic
```

* Then submit benchmarks to Whale by executing the following commands:
```
$ storm jar benchmark-didiOrderMatch-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelBalancedParitalBenchTopology BenchTopology <ordersTopic> <numofMachines> 1 <parallelismLevels> 3 rdma 1  
$ storm jar benchmark-stockDeal-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.stockdeal.StockDealBalancedPartialBenchTopology StockDeal <stockTopic> <numofMachines> 1 <parallelismLevels> 3 rdma 1
```

## License
Whale is released under the [Apache 2 license](http://www.apache.org/licenses/LICENSE-2.0.html).


### Publications
If you want to know more detailed information, please refer to this paper:

Jie Tan, Hanhua Chen, Yonghui Wang, Hai Jin, "Whale: Efficient One-to-Many Data Partitioning in RDMA-assisted Distributed Stream Processing Systems," Proceedings of International Conference for High Performance Computing, Networking, Storage and Analysis (SC 2021), St. Louis. MO, USA, Nov. 14-19, 2021. 
([bibtex](https://github.com/CGCL-codes/Whale/blob/master/Whale.bib))

### Authors and Copyright
Whale is developed in National Engineering Research Center for Big Data Technology and System, Cluster and Grid Computing Lab, Services Computing Technology and System Lab, School of Computer Science and Technology, Huazhong University of Science and Technology, Wuhan, China by Jie Tan (tjmaster@hust.edu.cn), Hanhua Chen (chen@hust.edu.cn), Yonghui Wang (yhw@hust.edu.cn), Hai Jin (hjin@hust.edu.cn).

Copyright (C) 2021, [STCS & CGCL](grid.hust.edu.cn) and [Huazhong University of Science and Technology](www.hust.edu.cn).
