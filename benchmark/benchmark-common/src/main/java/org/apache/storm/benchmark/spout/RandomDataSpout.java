package org.apache.storm.benchmark.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * locate org.apache.storm
 * Created by MasterTj on 2019/5/21.
 */
public class RandomDataSpout extends BaseRichSpout{
    private static Logger logger= LoggerFactory.getLogger(RandomDataSpout.class);
    public static final String SPOUT_STREAM_ID ="spout_stream";
    private SpoutOutputCollector outputCollector;
    private ConcurrentHashMap<UUID,Values> pending; //用来记录tuple的msgID，和tuple
    private boolean m_bool=true;

    private Long totalTuples;

    public RandomDataSpout(Long totalTuples) {
        this.totalTuples = totalTuples;
    }

    private String getRandomOredersData(){
        String ordaersData="8c9dcba50533612757f195d6e0540b54,1480331793,f07cd8e357a05434a4650459bfa180c3";
        return ordaersData;
    }

    //初始化操作
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        logger.info("------------SentenceThroughputSpout open------------");
        this.outputCollector=spoutOutputCollector;
        this.pending=new ConcurrentHashMap<UUID, Values>();
    }

    //核心逻辑
    public void nextTuple() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(m_bool) {
            for (long i = 1; i <= totalTuples; i++) {
                String topic = "ordersTopic";
                Integer partition = 888;
                String key = "null";
                String value = getRandomOredersData();
                //System.out.println(value);
                Values values = new Values(topic, partition, i, key, value, System.currentTimeMillis());
                UUID uuid = UUID.randomUUID();
                outputCollector.emit(SPOUT_STREAM_ID, values, uuid);
                pending.put(uuid, values);
            }
            m_bool=false;
        }
    }

    //Storm 的消息ack机制
    @Override
    public void ack(Object msgId) {
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        //System.err.println("fail"+msgId);
        //outputCollector.emit(SPOUT_STREAM_ID,pending.get(msgId),msgId);
    }

    //向下游输出
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(SPOUT_STREAM_ID,new Fields("topic", "partition", "offset", "key", "value","timeinfo"));
    }

    @Override
    public void close() {
    }

}
