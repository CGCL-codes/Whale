package org.apache.storm.possion;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.poisson.KafkaPoissonSpout;
import org.apache.storm.poisson.PoissonSpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import java.util.*;

public class PossionBenchTopology {
    private static final String REPORT_STREAM = "REPORT_STREAM";
    private static final String SENTENCE_STREAM = "SENTENCE_STREAM";

    public static class PSentenceSpout extends PoissonSpout {
        private SpoutOutputCollector collector;
        private Random random;
        private String[] sentences;
        final private static String REPORT_STREAM = "REPORT_STREAM";

        private int count=0;
        public PSentenceSpout(int expect) {
            super(expect);
        }

        @Override
        public void next() {
            final String sentence = sentences[random.nextInt(sentences.length)];
            count++;
            collector.emit(new Values(sentence), UUID.randomUUID());
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            this.random = new Random();
            sentences = new String[]{
                    "the cow jumped over the moon",
                    "an apple a day keeps the doctor away",
                    "four score and seven years ago",
                    "snow white and the seven dwarfs",
                    "i am at two with nature"
            };
            Timer countPerSecond = new Timer();
            countPerSecond.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    collector.emit(REPORT_STREAM, new Values(count, e));
                    count = 0;
                }
            }, 10, 1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
            declarer.declareStream(REPORT_STREAM, new Fields("count", "expect"));
        }
    }

    public static class KafkaSentenceSpout<K, V> extends KafkaPoissonSpout<K, V> {
        private static final String REPORT_STREAM = "REPORT_STREAM";
        private static final String SENTENCE_STREAM = "SENTENCE_STREAM";
        private int count = 0;

        public KafkaSentenceSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig, int expect) {
            super(kafkaSpoutConfig, expect);
        }

        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            super.open(conf, context, collector);
            super.setCounter(() -> count++);
            Timer countPerSecond = new Timer();
            countPerSecond.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    collector.emit(REPORT_STREAM, new Values(count));
                    count = 0;
                }
            }, 10, 1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream(REPORT_STREAM, new Fields("count"));
            declarer.declareStream(SENTENCE_STREAM, new Fields("sentence"));
        }
    }


    public static class SplitBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String sentence = input.getString(0);
            for (String word : sentence.split("\\s+")) {
                collector.emit(new Values(word, 1));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static class ReportBolt extends BaseBasicBolt {
        final private static String REPORT_STREAM = "REPORT_STREAM";

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            if (input.getSourceStreamId().equals(REPORT_STREAM)) {
                System.out.println("countPerSecond: " + input.getInteger(0));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    @Test
    public void testPossionSpoutBenchmark() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new PSentenceSpout(50000));
        builder.setBolt("split", new SplitBolt(), 2).shuffleGrouping("spout");
        builder.setBolt("report", new ReportBolt()).shuffleGrouping("spout", REPORT_STREAM);

        String topologyName = "possion-bench";
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumAckers(0);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builder.createTopology());
        Utils.sleep(1000 * 30);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }

    @Test
    public void testKafkaPossionSpoutBenchmark() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpoutConfig spoutConfig = KafkaSpoutConfig.builder("localhost:9092", "stormKafka")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "StormKafkaTest")
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST)
                .setRecordTranslator((Func<ConsumerRecord<String, String>, List<Object>>) record -> new Values(record.value()), new Fields("sentence"), SENTENCE_STREAM)
                .build();

        builder.setSpout("kafkaSpout", new KafkaSentenceSpout<>(spoutConfig, 60000));

        builder.setBolt("split", new SplitBolt(), 2).shuffleGrouping("kafkaSpout", SENTENCE_STREAM);
        builder.setBolt("report", new ReportBolt()).shuffleGrouping("kafkaSpout", REPORT_STREAM);

        String topologyName = "kafka-possion-bench";
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumAckers(0);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builder.createTopology());
        Utils.sleep(1000 * 30);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }
}
