package org.apache.storm.multicast.model;

import org.apache.storm.multicast.core.SchedulingTopologyBuilder;
import org.apache.storm.multicast.io.ExportException;
import org.apache.storm.multicast.io.ImportException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseComponent;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.apache.storm.multicast.model.BalancedPartialMulticastGraphTest.TestSpout.SPOUT_STREAM_ID;

/**
 * locate org.apache.storm.multicast.model
 * Created by master on 2019/10/13.
 */
public class BalancedPartialMulticastGraphTest {
    private static Logger logger= LoggerFactory.getLogger(BalancedPartialMulticastGraphTest.class);

    private SchedulingTopologyBuilder builder= new SchedulingTopologyBuilder();

    public static class TestSpout extends BaseComponent implements IRichSpout {
        private SpoutOutputCollector collector;
        private Random random;
        private String[] sentences;
        public static final String SPOUT_STREAM_ID ="spoutstream";
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
            collector.emit(SPOUT_STREAM_ID, new Values(sentence), UUID.randomUUID());
        }

        @Override
        public void ack(Object msgId) {

        }

        @Override
        public void fail(Object msgId) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream(SPOUT_STREAM_ID,new Fields("word"));
        }
    }

    public static class BroadcastBolt extends BaseRichBolt {
        public static final String BROADCAST_STREAM_ID ="broadcaststream";
        private OutputCollector collector;

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream(BROADCAST_STREAM_ID,new Fields("word", "count"));
        }

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector= collector;
        }

        @Override
        public void execute(Tuple input) {
            String sentence = input.getString(0);
            for (String word : sentence.split("\\s+")) {
                collector.emit(BROADCAST_STREAM_ID, new Values(word, 1));
            }
        }
    }

    @Test
    public void graphToJson() throws ExportException {
        BalancedPartialMulticastGraph<String> dag = null;

        builder.setSpout("testSpout", new TestSpout());
        dag = builder.constructionSequential(34, 10,"testSpout", SPOUT_STREAM_ID, BroadcastBolt.class);

        logger.info("graphJosn: {}",BalancedPartialMulticastGraph.graphToJson(dag));
    }

    @Test
    public void jsonToGraph() throws ExportException, ImportException {

        BalancedPartialMulticastGraph<String> dag = null;

        builder.setSpout("testSpout", new TestSpout());
        dag = builder.constructionSequential(34, 10,"testSpout", SPOUT_STREAM_ID, BroadcastBolt.class);

        String json = BalancedPartialMulticastGraph.graphToJson(dag);

        BalancedPartialMulticastGraph<String> objectBalancedPartialMulticastGraph = BalancedPartialMulticastGraph.jsonToGraph(json);
        logger.info("multicastGraph: {}", objectBalancedPartialMulticastGraph);
    }
}
