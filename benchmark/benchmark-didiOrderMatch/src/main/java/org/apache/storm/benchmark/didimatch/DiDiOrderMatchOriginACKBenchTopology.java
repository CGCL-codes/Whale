package org.apache.storm.benchmark.didimatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.Constraints;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.benchmark.bolt.OrderMatchBolt;
import org.apache.storm.benchmark.spout.OrderMatchPossionKafkaSpout;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.report.BenchReportBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import static org.apache.storm.Constraints.*;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

/**
 * locate org.apache.storm.starter
 * Created by mastertj on 2018/3/5.
 * DiDi滴滴打车订单匹配Topology
 * storm jar benchmark-didiOrderMatch-2.0.0-SNAPSHOT.jar org.apache.storm.benchmark.didimatch.DiDiOrderMatchOriginACKBenchTopology DiDiOrderMatchTopology ordersTopic 24 1 23 100000 netty 1
 */
public class DiDiOrderMatchOriginACKBenchTopology {

    public static void main(String[] args) throws Exception{
        String topologyName=args[0];
        String topic=args[1];
        Integer numworkers=Integer.valueOf(args[2]);
        Integer spoutInstancesNum=Integer.valueOf(args[3]);
        Integer boltInstancesNum=Integer.valueOf(args[4]);
        Integer possionExpect=Integer.valueOf(args[5]);
        String transport = args[6];
        Integer sendTimeLimit=Integer.valueOf(args[7]);
        TopologyBuilder builder=new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT_ID, new OrderMatchPossionKafkaSpout<>(getKafkaSpoutConfig(KAFKA_LOCAL_BROKER, topic), possionExpect), spoutInstancesNum);
        builder.setBolt(DIDIMATCH_BOLT_ID, new OrderMatchBolt(),boltInstancesNum).allGrouping(KAFKA_SPOUT_ID,SPOUT_STREAM_ID);
        builder.setBolt(BENCH_BOLT_ID,new BenchReportBolt()).shuffleGrouping(KAFKA_SPOUT_ID,LATENCY_STREAM_ID)
                .shuffleGrouping(KAFKA_SPOUT_ID,THROUGHPUT_STREAM_ID);

        Config config=new Config();
        //config.setDebug(true);

        //RDMA Configuration
        config.put(Config.STORM_MESSAGING_TRANSPORT, "org.apache.storm.messaging."+transport+".Context");
        config.put(Config.STORM_MESSAGING_RDMA_SEND_LIMIT_TIME, sendTimeLimit);
        config.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, false);

        //BenchMark Configuration
        config.put(TOPOLOGY_WORKERS_NUM, numworkers);
        config.put(Constraints.BOLT_INSTANCES_NUM, boltInstancesNum);
        config.put(Constraints.BOLT_MAXINSTANCE_NUM, numworkers * 16); // Machine (16 cores)

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
                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value(),System.currentTimeMillis()),
                new Fields("topic", "partition", "offset", "key", "value", "timeinfo"), SPOUT_STREAM_ID);
//        trans.forTopic(TOPIC_2,
//                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
//                new Fields("topic", "partition", "offset", "key", "value"), TOPIC_2_STREAM);
        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{topic})
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "DiDiOrderMatchBenchGroup")
                .setRetry(getRetryService())
                .setRecordTranslator(trans)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .setFirstPollOffsetStrategy(EARLIEST)
                .build();
    }

    public static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(0),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }
}
