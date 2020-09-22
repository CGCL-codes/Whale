package org.apache.storm.benchmark.bolt;

import org.apache.storm.Constraints;
import org.apache.storm.benchmark.BenchThroughputBolt;
import org.apache.storm.generated.Grouping;
import org.apache.storm.multicast.model.MulticastID;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.storm.Constraints.START_TIME_MILLS;
import static org.apache.storm.Constraints.THROUGHPUT_STREAM_ID;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 */
public class OrderMatchThroughputBolt extends BenchThroughputBolt {
    private static Logger logger= LoggerFactory.getLogger(OrderMatchThroughputBolt.class);

    private int thisTaskId = 0;
    private Long boltInstancesNum;
    private boolean isOutputMetrics= true;
    private long matchTimes;
    private Map<Integer,Object> driverGPSMap = new LinkedHashMap<>();

    private Map<String, Map<String, Grouping>> thisTargets;
    private MulticastID multicastID;
    private OutputCollector outputCollector;
    private static final String BROADCAST_STREAM_ID ="broadcaststream";

    public OrderMatchThroughputBolt() {
        super( 1000);
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        super.prepare(topoConf,context,collector);

        this.outputCollector=collector;
        this.thisTaskId = context.getThisTaskId();
        this.thisTargets = context.getThisTargets();
        this.multicastID = new MulticastID(context.getThisComponentId());
        this.boltInstancesNum = (Long) topoConf.get(Constraints.BOLT_INSTANCES_NUM);

        Long boltMaxInstancesNum = (Long) topoConf.get(Constraints.BOLT_MAXINSTANCE_NUM);
        this.matchTimes = (long) (Constraints.DIDI_MATCH_LATECNY * Constraints.DIDI_MATCH_LINE *
                (boltMaxInstancesNum - (double)boltInstancesNum) / boltMaxInstancesNum );

        // 防止中间MulticastBenchBolt 一直在输出Metrics数据
        if(!thisTargets.containsKey(THROUGHPUT_STREAM_ID)){
            this.getThroughputTimer().cancel();
            isOutputMetrics=false;
        }

        for (int i = 0; i < matchTimes; i++) {
            driverGPSMap.put(i, this);
        }
    }

    @Override
    public void execute(Tuple input) {
        Long startTimeMills=input.getLongByField(START_TIME_MILLS);
        String topic=input.getStringByField("topic");
        Integer partition=input.getIntegerByField("partition");
        Long offset=input.getLongByField("offset");
        String key=input.getStringByField("key");
        String value=input.getStringByField("value");

        if(isOutputMetrics)
            super.nextThroughputTuple(input);

        // do something your coding logic
        logger.debug("matchTimes:{}", matchTimes);
        for (Integer integer : driverGPSMap.keySet()) {

        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        super.declareOutputFields(declarer);
        declarer.declareStream(BROADCAST_STREAM_ID,new Fields("topic","partition","offset","key","value", START_TIME_MILLS));
    }
}
