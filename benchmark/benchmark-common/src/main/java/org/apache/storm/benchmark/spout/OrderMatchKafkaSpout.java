package org.apache.storm.benchmark.spout;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 * 重写KafkaSpout
 */
public class OrderMatchKafkaSpout<K, V> extends KafkaSpout<K, V> {
    private int thisTaskId =0;

    public OrderMatchKafkaSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        super(kafkaSpoutConfig);
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);

        thisTaskId=context.getThisTaskId();
    }

    @Override
    public void nextTuple() {
        super.nextTuple();

    }

    @Override
    public void ack(Object messageId) {
        super.ack(messageId);
    }

    @Override
    public void fail(Object messageId) {
        super.fail(messageId);
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        super.declareOutputFields(outputFieldsDeclarer);
    }

}
