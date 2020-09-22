package org.apache.storm.benchmark.bolt;

import org.apache.storm.Constraints;
import org.apache.storm.benchmark.model.DiDiOrder;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.storm.Constraints.BROADCAST_STREAM_ID;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 */
public class OrderMatchBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(OrderMatchBolt.class);

    private int thisTaskId = 0;
    private Long boltInstancesNum;
    private boolean isOutputMetrics= true;
    private long matchTimes;
    private Map<Integer,Object> driverGPSMap = new LinkedHashMap<>();

    private Map<String, Map<String, Grouping>> thisTargets;
    protected OutputCollector outputCollector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector=collector;
        this.thisTaskId = context.getThisTaskId();
        this.thisTargets = context.getThisTargets();
        this.boltInstancesNum = (Long) topoConf.get(Constraints.BOLT_INSTANCES_NUM);

        Long boltMaxInstancesNum = (Long) topoConf.get(Constraints.BOLT_MAXINSTANCE_NUM);
        this.matchTimes = (long) (Constraints.DIDI_MATCH_LATECNY * Constraints.DIDI_MATCH_LINE *
                (boltMaxInstancesNum - (double)boltInstancesNum) / boltMaxInstancesNum );

        for (int i = 0; i < matchTimes; i++) {
            driverGPSMap.put(i, this);
        }
    }

    @Override
    public void execute(Tuple input) {
        String topic=input.getStringByField("topic");
        Integer partition=input.getIntegerByField("partition");
        Long offset=input.getLongByField("offset");
        String key=input.getStringByField("key");
        String value=input.getStringByField("value");
        String[] strs=value.split(",");
        DiDiOrder order=new DiDiOrder(strs[0],strs[1],strs[2]);

        // do something your coding logic
        logger.debug("matchTimes:{}", matchTimes);
        for (Integer integer : driverGPSMap.keySet()) {

        }

        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(BROADCAST_STREAM_ID,new Fields("topic","partition","offset","key","value"));
    }

    @Override
    public void cleanup() {

    }
}
