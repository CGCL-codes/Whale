package org.apache.storm.benchmark.spout;

import org.apache.storm.benchmark.BenchACKSpout;
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
public class StockDealBenchKafkaSpout<K, V> extends BenchACKSpout<K, V> {
    private int thisTaskId =0;

    public StockDealBenchKafkaSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        super(kafkaSpoutConfig,1000,1000);
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
