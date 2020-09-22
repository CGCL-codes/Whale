package org.apache.storm.benchmark.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static org.apache.storm.Constraints.FORWARD_STREAM_ID;
import static org.apache.storm.Constraints.START_TIME_MILLS;

/**
 * locate org.apache.storm.benchmark.multicast.bolt
 * Created by MasterTj on 2019/10/20.
 */
public class MulticastForwardBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        String topic=input.getStringByField("topic");
        Integer partition=input.getIntegerByField("partition");
        Long offset=input.getLongByField("offset");
        String key=input.getStringByField("key");
        String value=input.getStringByField("value");

        collector.emit(FORWARD_STREAM_ID,new Values(topic,partition,offset,key,value,System.currentTimeMillis()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(FORWARD_STREAM_ID,new Fields("topic","partition","offset","key","value", START_TIME_MILLS));
    }
}
