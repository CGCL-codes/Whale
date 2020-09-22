package org.apache.storm.benchmark.multicast;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.*;
import org.apache.storm.benchmark.bolt.MulticastBenchBolt;
import org.apache.storm.benchmark.bolt.MulticastForwardBolt;
import org.apache.storm.benchmark.spout.MulticastPossionKafkaSpout;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.multicast.core.MulticastController;
import org.apache.storm.multicast.core.SchedulingTopologyBuilder;
import org.apache.storm.multicast.model.BalancedPartialMulticastGraph;
import org.apache.storm.multicast.util.ModelConstants;
import org.apache.storm.report.BenchReportBolt;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.storm.Constraints.*;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

/**
 * locate org.apache.storm.starter
 * Created by mastertj on 2018/3/5.
 * 流广播模型Benchmark Topology
 * storm jar benchmark-multicastModel-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.multicast.MulticastModelBalancedParitalForwardBenchTopology MulticastBenchTopology ordersTopic 26 1 25 10 100000 rdma 1
 * 平均吞吐量 30 4 64027.0426 (并行度 最大出度 吞吐量)
 */
public class MulticastModelBalancedParitalForwardBenchTopology {

    public static void main(String[] args) throws Exception{
        String topologyName=args[0];
        String topic=args[1];
        Integer numworkers=Integer.valueOf(args[2]);
        Integer spoutInstancesNum=Integer.valueOf(args[3]);
        Integer boltInstancesNum=Integer.valueOf(args[4]);
        Integer maxOutDegree=Integer.valueOf(args[5]);
        Integer possionExpect=Integer.valueOf(args[6]);
        String transport = args[7];
        Integer sendTimeLimit=Integer.valueOf(args[8]);
        SchedulingTopologyBuilder builder=new SchedulingTopologyBuilder();
        BalancedPartialMulticastGraph<String> graph = null;

        //根据当前流输入速率，计算当前流多播模型的最大出度
        if(maxOutDegree <= 0){
            maxOutDegree = MulticastController.computeMaxOutDegree(possionExpect, ModelConstants.queueCapacity);
        }

        Map<String,String> directSchedulerMap=new HashMap<>();
        int indexWorker = 0;
        directSchedulerMap.put(KAFKA_SPOUT_ID, SCHEDULER_SPECIAL_SUPERVISOR + (indexWorker++));
        directSchedulerMap.put(FORWARD_BOLT_ID, SCHEDULER_SPECIAL_SUPERVISOR + (indexWorker++));

        builder.setSpout(KAFKA_SPOUT_ID, new MulticastPossionKafkaSpout<>(getKafkaSpoutConfig(KAFKA_LOCAL_BROKER, topic), possionExpect), spoutInstancesNum);
        builder.setBolt(FORWARD_BOLT_ID, new MulticastForwardBolt() , spoutInstancesNum).shuffleGrouping(KAFKA_SPOUT_ID, SPOUT_STREAM_ID);
        graph = builder.constructionBalancedPartialMulticast(boltInstancesNum, numworkers - 2, maxOutDegree,
                FORWARD_BOLT_ID, FORWARD_STREAM_ID, BROADCAST_STREAM_ID, MulticastBenchBolt.class);

        Iterator<String> iterator = graph.vertexSet().iterator();
        BoltDeclarer boltDeclarer = builder.setBolt(BENCH_BOLT_ID, new BenchReportBolt(), 1);
        while (iterator.hasNext()){
            String bolt_ID = iterator.next();
            if(bolt_ID.startsWith(BROADCAST_BOLT)) {
                directSchedulerMap.put(bolt_ID, SCHEDULER_SPECIAL_SUPERVISOR + (indexWorker++));

                boltDeclarer.shuffleGrouping(bolt_ID, Constraints.LATENCY_STREAM_ID)
                        .shuffleGrouping(bolt_ID, Constraints.THROUGHPUT_STREAM_ID);
            }
        }

        //List<String> elementList = graph.getElementByLayer(graph.getLayer());
        //builder.setBolt(BENCH_BOLT_ID, new BenchReportBolt(), 1);
        //String elementBolt= elementList.get(elementList.size()-1);
        directSchedulerMap.put(BENCH_BOLT_ID, SCHEDULER_SPECIAL_SUPERVISOR + (--indexWorker));

        Config config=new Config();
        config.setNumAckers(0);
        //config.setDebug(true);

        //Multicast Configuration
        config.put(Constants.Balanced_Partial_MulticastGraph, BalancedPartialMulticastGraph.graphToJson(graph));
        config.put(Config.TOPOLOGY_MULTICAST_SOURCE_STREMAID,FORWARD_STREAM_ID);
        config.put(Config.TOPOLOGY_MULTICAST_BROADCASTBOLT_STREMAID,BROADCAST_STREAM_ID);

        //RDMA Configuration
        config.put(Config.STORM_MESSAGING_TRANSPORT, "org.apache.storm.messaging."+transport+".Context");
        config.put(Config.STORM_MESSAGING_RDMA_SEND_LIMIT_TIME, sendTimeLimit);
        config.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);

        // Whale-Multicast DirectScheduler
        config.put("assigned_flag", "1");
        config.put("design_map", directSchedulerMap);

        //Whale-Multicast NonBlockingMulticastTree 打开动态自我调节机制策略
        //config.put(Config.TOPOLOGY_NONBLOCKMULIT_ENABLE, true);
        config.put("nonblockMultiTree",graph);

        if(args!=null && args.length <= 0){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, builder.createTopology());
            Utils.sleep(1000 * 50);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }else {
            config.setNumWorkers(numworkers);//每一个Worker 进程默认都会对应一个Acker 线程
            StormSubmitter.submitTopology(topologyName,config,builder.createTopology());
        }
    }

    public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers,String topic) {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
                new Fields("topic", "partition", "offset", "key", "value"), SPOUT_STREAM_ID);
//        trans.forTopic(TOPIC_2,
//                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
//                new Fields("topic", "partition", "offset", "key", "value"), TOPIC_2_STREAM);
        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{topic})
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "MulticastModelBenchGroup")
                .setRetry(getRetryService())
                .setRecordTranslator(trans)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
                .setFirstPollOffsetStrategy(EARLIEST)
                .build();
    }

    public static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(0),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }
}
