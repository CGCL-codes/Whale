package org.apache.storm.benchmark.didimatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.*;
import org.apache.storm.benchmark.bolt.OrderMatchThroughputBolt;
import org.apache.storm.benchmark.spout.OrderMatchKafkaSpout;
import org.apache.storm.benchmark.spout.OrderMatchPossionKafkaSpout;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.multicast.core.SchedulingTopologyBuilder;
import org.apache.storm.multicast.model.BalancedPartialMulticastGraph;
import org.apache.storm.report.ThroughputReportBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.storm.Constraints.*;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

/**
 * locate org.apache.storm.starter
 * Created by mastertj on 2018/3/5.
 * 流广播模型Benchmark Topology
 storm jar benchmark-didiOrderMatch-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.didimatch.DiDiOrderMatchSequentialThroughputTopology \
 MulticastThroughputTopology ordersTopic6 17 1 272 rdma 1 60000
 */
public class DiDiOrderMatchSequentialThroughputTopology {

    public static void main(String[] args) throws Exception{
        String topologyName=args[0];
        String topic=args[1];
        Integer numworkers=Integer.valueOf(args[2]);
        Integer spoutInstancesNum=Integer.valueOf(args[3]);
        Integer boltInstancesNum=Integer.valueOf(args[4]);
        String transport = args[5];
        Integer sendTimeLimit=Integer.valueOf(args[6]);
        int streamRate = Integer.valueOf(args[7]);

        SchedulingTopologyBuilder builder=new SchedulingTopologyBuilder();
        BalancedPartialMulticastGraph<String> graph = null;

        Map<String,String> directSchedulerMap=new HashMap<>();
        int indexWorker = 0;
        directSchedulerMap.put(KAFKA_SPOUT_ID, SCHEDULER_SPECIAL_SUPERVISOR + (indexWorker++));

        builder.setSpout(KAFKA_SPOUT_ID, new OrderMatchPossionKafkaSpout<>(getKafkaSpoutConfig(KAFKA_LOCAL_BROKER, topic), streamRate), spoutInstancesNum);
//        builder.setSpout(KAFKA_SPOUT_ID, new OrderMatchKafkaSpout<>(getKafkaSpoutConfig(KAFKA_LOCAL_BROKER, topic)), spoutInstancesNum);
        graph = builder.constructionSequential(boltInstancesNum, numworkers - 1,
                KAFKA_SPOUT_ID, SPOUT_STREAM_ID, OrderMatchThroughputBolt.class);

        Iterator<String> iterator = graph.vertexSet().iterator();
        while (iterator.hasNext()){
            String bolt_ID = iterator.next();
            if(bolt_ID.startsWith(BROADCAST_BOLT))
                directSchedulerMap.put(bolt_ID, SCHEDULER_SPECIAL_SUPERVISOR + (indexWorker++));
        }

        List<String> elementList = graph.getElementByLayer(graph.getLayer());
        String elementBolt= elementList.get(elementList.size()-1);
        builder.setBolt(THROUGHPUT_BOLT_ID, new ThroughputReportBolt(),1).shuffleGrouping(elementBolt, THROUGHPUT_STREAM_ID);
        directSchedulerMap.put(THROUGHPUT_BOLT_ID, SCHEDULER_SPECIAL_SUPERVISOR + (--indexWorker));

        Config config=new Config();
        config.setNumAckers(0);
        //config.setDebug(true);

        //Multicast Configuration
        config.put(Constants.Balanced_Partial_MulticastGraph, BalancedPartialMulticastGraph.graphToJson(graph));
        config.put(Config.TOPOLOGY_MULTICAST_SOURCE_STREMAID,SPOUT_STREAM_ID);
        config.put(Config.TOPOLOGY_MULTICAST_BROADCASTBOLT_STREMAID,BROADCAST_STREAM_ID);

        //RDMA Configuration
        config.put(Config.STORM_MESSAGING_TRANSPORT,"org.apache.storm.messaging."+transport+".Context");
        config.put(Config.STORM_MESSAGING_RDMA_SEND_LIMIT_TIME,sendTimeLimit);

        //BenchMark Configuration
        config.put(Constraints.BOLT_INSTANCES_NUM, boltInstancesNum);
        config.put(Constraints.BOLT_MAXINSTANCE_NUM, numworkers * 16); // Machine (16 cores)

        // Whale-Multicast DirectScheduler
        config.put("assigned_flag", "1");
        config.put("design_map", directSchedulerMap);
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
                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value(), System.currentTimeMillis()),
                new Fields("topic", "partition", "offset", "key", "value", Constraints.START_TIME_MILLS), SPOUT_STREAM_ID);
//        trans.forTopic(TOPIC_2,
//                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
//                new Fields("topic", "partition", "offset", "key", "value"), TOPIC_2_STREAM);
        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{topic})
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "MulticastModelThroughputGroup")
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
