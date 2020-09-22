package org.apache.storm.benchmark;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.storm.Constraints.*;

/**
 * Bolt 测试Storm吞吐量
 * locate org.apache.storm.benchmark
 * Created by master on 2019/10/14.
 */
public class BenchThroughputBolt extends BaseRichBolt {
    private int throughputTimerPeriod;

    private Timer throughputTimer;
    private long throughputCount=0; //记录单位时间ACK的元组数量
    private int thisTaskId = 0;

    protected OutputCollector outputCollector;

    public BenchThroughputBolt(int throughputTimerPeriod) {
        this.throughputTimerPeriod = throughputTimerPeriod;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.thisTaskId = context.getThisTaskId();
        this.outputCollector=collector;
        this.throughputTimer = new Timer();

        //设置计时器没1s计算时间
        throughputTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //将最后结果输出到日志文件中
                outputCollector.emit(THROUGHPUT_STREAM_ID,new Values(thisTaskId,throughputCount,System.currentTimeMillis()));
                throughputCount = 0;
            }
        }, 10,throughputTimerPeriod);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void execute(Tuple input) {

    }

    protected void nextThroughputTuple(Tuple tuple){
        throughputCount++;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(THROUGHPUT_STREAM_ID,new Fields(TASK_ID,THROUGHPUT_STREAM_COUNT,TIME_INFO));
    }

    public Timer getThroughputTimer() {
        return throughputTimer;
    }
}
