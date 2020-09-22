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
 * 用来统计输出Spout输入源的吞吐量的Bolt
 */
public class ThroughputReportBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(ThroughputReportBolt.class);

    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
        this.outputCollector=outputCollector;
        logger.info("------------ThroughputReportBolt prepare------------");
    }

    public void execute(Tuple tuple) {
        //将最后结果输出到数据库中
        try {
            Long timeinfo = tuple.getLongByField(Constraints.TIME_INFO);
            Long tuplecount = tuple.getLongByField(Constraints.THROUGHPUT_STREAM_COUNT);
            int taskid = tuple.getIntegerByField(Constraints.TASK_ID);

            logger.debug("taskid: {}, timeinfo:{}, tuplecount:{}", taskid,timeinfo,tuplecount);
            DataBaseUtil.insertDiDiThroughput(taskid,tuplecount,new Timestamp(timeinfo));
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
