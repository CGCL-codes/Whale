package org.apache.storm.starter;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * locate org.apache.storm.starter
 * Created by tjmaster on 18-2-1.
 * 实时流处理系统高校序列化AllGrouping优化测试Topology
 * storm jar storm-starter-2.0.0-SNAPSHOT.jar org.apache.storm.starter.AllGroupingTestTopology allgrouping
 */
public class AllGroupingTestTopology extends ConfigurableTopology{
    public static class RandomWordSpout extends BaseRichSpout {
        private ConcurrentHashMap<UUID, Values> pending;
        private static final Logger LOG = LoggerFactory.getLogger(RandomWordSpout.class);
        private SpoutOutputCollector outputCollector;
        private String[] strings={"aaa","bbb","ccc","ddd","fff"};
        private int index=0;
        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.outputCollector=collector;
            this. pending = new ConcurrentHashMap<UUID, Values>();
        }

        @Override
        public void nextTuple() {
            if(index<=strings.length){
                UUID msgId = UUID.randomUUID();
                Values value = new Values(strings[index % strings.length]);

                LOG.info("the time of emitting tuple : {} {}", value.get(0), System.currentTimeMillis());
                this.outputCollector.emit(value,msgId);    //read a line, emit as a word
                this.pending.put(msgId,value);
                index++;
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public void ack(Object msgId) {
            pending.remove(msgId);
        }

        @Override
        public void fail(Object msgId) {
            this.outputCollector.emit(pending.get(msgId),msgId);
        }
    }

    public static class FirstBolt extends BaseBasicBolt {
        private static final Logger LOG = LoggerFactory.getLogger(FirstBolt.class);

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String word = input.getStringByField("word");
            LOG.info("the time of emitting tuple : {} {}", word, System.currentTimeMillis());
            collector.emit(new Values("first "+word));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("first"));
        }
    }

    public static class SecondBolt extends BaseBasicBolt {
        private static final Logger LOG = LoggerFactory.getLogger(SecondBolt.class);

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String word = input.getStringByField("first");
            LOG.info("the time of emitting tuple : {} {}", word, System.currentTimeMillis());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }

    public static void main(String[] args) throws Exception {
        ConfigurableTopology.start(new AllGroupingTestTopology(), args);
    }

    protected int run(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomWordSpout(), 1);

        builder.setBolt("first", new FirstBolt(), 3).allGrouping("spout");

        builder.setBolt("second", new SecondBolt(), 6).allGrouping("first");

        conf.setDebug(true);

        String topologyName = "AllGroupingTestTopology";

        conf.setNumWorkers(4);

        if (args != null && args.length > 0) {
            topologyName = args[0];
        }
        return submit(topologyName, conf, builder);
    }
}
