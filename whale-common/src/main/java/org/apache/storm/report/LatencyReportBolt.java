package org.apache.storm.report;

import org.apache.storm.Constraints;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.util.DataBaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Map;

/**
 * Created by 79875 on 2017/3/7.
 * 用来统计输出Spout输入源的延迟的Bolt
 */
public class LatencyReportBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(LatencyReportBolt.class);

    private OutputCollector outputCollector;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
        this.outputCollector=outputCollector;
        logger.info("------------LatencyReportBolt prepare------------");
    }

    public void execute(Tuple tuple) {
        //将最后结果输出到数据库文件中
        try {
            Integer taskid = tuple.getIntegerByField(Constraints.TASK_ID);
            Double avgDelay = tuple.getDoubleByField(Constraints.LATENCY_STREAM_AVGLATENCY);
            Long timeinfo = tuple.getLongByField(Constraints.TIME_INFO);

            logger.debug("taskid: {}, timeinfo:{}, avgDelay:{}", taskid,timeinfo,avgDelay);
            DataBaseUtil.insertDiDiLatency(taskid,avgDelay,new Timestamp(timeinfo));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
    }
}
