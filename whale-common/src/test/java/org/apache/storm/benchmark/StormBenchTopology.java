package org.apache.storm.benchmark;

import org.apache.storm.Config;
import org.apache.storm.Constraints;
import org.apache.storm.LocalCluster;
import org.apache.storm.report.BenchReportBolt;
import org.apache.storm.report.LatencyReportBolt;
import org.apache.storm.report.ThroughputReportBolt;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * locate org.apache.storm.benchmark
 * Created by master on 2019/10/14.
 */
public class StormBenchTopology {

    public static final String SPOUT_STREAM_ID ="spoutstream";

    public static class TestSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private Random random;
        private String[] sentences;
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
        }

        @Override
        public void nextTuple() {
            final String sentence = sentences[random.nextInt(sentences.length)];
            count++;
            collector.emit(SPOUT_STREAM_ID, new Values(sentence,System.currentTimeMillis()), UUID.randomUUID());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream(SPOUT_STREAM_ID,new Fields("word",Constraints.START_TIME_MILLS));
        }
    }

    public static class BenchThroughputBolt extends org.apache.storm.benchmark.BenchThroughputBolt {
        public static final String BENCH_STREAM_ID ="benchstream";

        public BenchThroughputBolt(int throughputTimerPeriod) {
            super(throughputTimerPeriod);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            //bench throughput
            super.declareOutputFields(declarer);
        }

        @Override
        public void execute(Tuple input) {
            //bench throughput
            super.nextThroughputTuple(input);

            String sentence = input.getString(0);
            for (String word : sentence.split("\\s+")) {
                //this.outputCollector.emit(BENCH_STREAM_ID, new Values(word, 1));
            }
        }
    }

    public static class BenchLatencyBolt extends org.apache.storm.benchmark.BenchLatencyBolt {
        public static final String BENCH_STREAM_ID ="benchstream";

        public BenchLatencyBolt(int throughputTimerPeriod) {
            super(throughputTimerPeriod);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            //bench latency
            super.declareOutputFields(declarer);

            declarer.declareStream(BENCH_STREAM_ID,new Fields("word", "count"));
        }

        @Override
        public void execute(Tuple input) {
            String sentence = input.getString(0);
            for (String word : sentence.split("\\s+")) {
                //this.outputCollector.emit(BENCH_STREAM_ID, new Values(word, 1));
            }

            //bench latency
            super.nextLatencyTuple(input);
        }
    }

    public static class TestBenchBolt extends BenchBolt {
        public static final String BENCH_STREAM_ID ="benchstream";

        public TestBenchBolt(int latencyTimerPeriod, int throughputTimerPeriod) {
            super(latencyTimerPeriod, throughputTimerPeriod);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            //bench throughput
            super.declareOutputFields(declarer);

            declarer.declareStream(BENCH_STREAM_ID,new Fields("word", "count"));
        }

        @Override
        public void execute(Tuple input) {
            //bench throughput
            super.nextThroughputTuple(input);

            String sentence = input.getString(0);
            for (String word : sentence.split("\\s+")) {
                //this.outputCollector.emit(BENCH_STREAM_ID, new Values(word, 1));
            }

            //bench latency
            super.nextLatencyTuple(input);
        }
    }

    @Test
    public void testThroughputBoltBenchmark() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("testSpout", new TestSpout());
        builder.setBolt("benchThroughputBolt", new BenchThroughputBolt(1000),1).shuffleGrouping("testSpout",SPOUT_STREAM_ID);
        builder.setBolt("report", new ThroughputReportBolt(),1).shuffleGrouping("benchThroughputBolt", Constraints.THROUGHPUT_STREAM_ID);

        String topologyName = "throughput-bench-topology";
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
    public void testLatencyBoltBenchmark() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("testSpout", new TestSpout());
        builder.setBolt("benchLatencyBolt", new BenchLatencyBolt(1000)).shuffleGrouping("testSpout",SPOUT_STREAM_ID);
        builder.setBolt("report", new LatencyReportBolt()).shuffleGrouping("benchLatencyBolt", Constraints.LATENCY_STREAM_ID);

        String topologyName = "latency-bench-topology";
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
    public void testBenchBoltBenchmark() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("testSpout", new TestSpout());
        builder.setBolt("benchBolt", new TestBenchBolt(1000,1000)).shuffleGrouping("testSpout",SPOUT_STREAM_ID);
        builder.setBolt("report", new BenchReportBolt()).shuffleGrouping("benchBolt", Constraints.LATENCY_STREAM_ID)
                .shuffleGrouping("benchBolt",Constraints.THROUGHPUT_STREAM_ID);

        String topologyName = "bench-topology";
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
