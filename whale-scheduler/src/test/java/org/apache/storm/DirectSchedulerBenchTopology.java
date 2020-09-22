package org.apache.storm;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseComponent;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import java.util.*;

/**
 * locate org.apache.storm
 * Created by MasterTj on 2019/10/11.
 */
public class DirectSchedulerBenchTopology {
    public static class TestSpout extends BaseComponent implements IRichSpout {
        private SpoutOutputCollector collector;
        private Random random;
        private String[] sentences;
        final private static String REPORT_STREAM = "REPORT_STREAM";

        private int count=0;

        public TestSpout() {
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
                    collector.emit(REPORT_STREAM, new Values(count));
                    count = 0;
                }
            }, 10, 1000);
        }

        @Override
        public void close() {
        }

        @Override
        public void activate() {
        }

        @Override
        public void deactivate() {
        }

        @Override
        public void nextTuple() {
            final String sentence = sentences[random.nextInt(sentences.length)];
            count++;
            collector.emit(new Values(sentence), UUID.randomUUID());
        }

        @Override
        public void ack(Object msgId) {

        }

        @Override
        public void fail(Object msgId) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
            declarer.declareStream(REPORT_STREAM, new Fields("count"));
        }
    }

    public static class TestBolt extends BaseBasicBolt {

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
    public void testDirectSchedulerBenchmark() throws Exception {
        String topologyName = "kafka-possion-bench";
        int numOfParallel;
        TopologyBuilder builder;
        StormTopology stormTopology;
        Config config;
        //待分配的组件名称与节点名称的映射关系
        HashMap<String, String> component2Node;

        //任务并行化数设为10个
        numOfParallel = 2;

        builder = new TopologyBuilder();

        String desSpout = "my_spout";
        String desBolt = "my_bolt";

        //设置spout数据源
        builder.setSpout(desSpout, new TestSpout(), numOfParallel);

        builder.setBolt(desBolt, new TestBolt(), numOfParallel)
                .shuffleGrouping(desSpout);

        config = new Config();
        config.setNumWorkers(numOfParallel);
        config.setMaxSpoutPending(65536);
        config.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 40000);
        config.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 40000);

        //LocalCluster cluster = new LocalCluster();
        //config.setDebug(true);
        //cluster.submitTopology("test-monitor", config, builder.createTopology());

        component2Node = new HashMap<>();

        component2Node.put(desSpout, "special-supervisor1");
        component2Node.put(desBolt, "special-supervisor2");

        //此标识代表topology需要被调度
        config.put("assigned_flag", "1");
        //具体的组件节点对信息
        config.put("design_map", component2Node);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, config, builder.createTopology());
        Utils.sleep(1000 * 30);
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }
}
