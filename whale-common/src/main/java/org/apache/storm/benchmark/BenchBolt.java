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
 * Bolt 测试Storm吞吐量和延迟
 * locate org.apache.storm.benchmark
 * Created by master on 2019/10/14.
 */
public class BenchBolt extends BaseRichBolt {
    private int latencyTimerPeriod;
    private int throughputTimerPeriod;

    private Timer latencyTimer;
    private Timer throughputTimer;
    protected OutputCollector outputCollector;

    //latency
    private int latencytuplecount=0;
    protected long totalDelay=0; //总和延迟 500个tuple
    private long startTimeMills;
    private int thisTaskId = 0;
    private Object latencyLock;

    //throughput
    protected long throughputCount=0; //记录单位时间ACK的元组数量

    public BenchBolt(int latencyTimerPeriod, int throughputTimerPeriod) {
        this.latencyTimerPeriod = latencyTimerPeriod;
        this.throughputTimerPeriod = throughputTimerPeriod;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.thisTaskId = context.getThisTaskId();
        this.outputCollector=collector;
        this.latencyTimer = new Timer();
        this.throughputTimer = new Timer();
        this.latencyLock = new Object();

        //设置计时器没1s计算时间
        latencyTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(startTimeMills!=0 && latencytuplecount!=0){
                    double avgDelay= ((double) totalDelay / (double) latencytuplecount);
                    avgDelay=(double) Math.round(avgDelay*100)/100;
                    collector.emit(LATENCY_STREAM_ID,new Values(thisTaskId,avgDelay,System.currentTimeMillis()));

                    synchronized (latencyLock) {
                        totalDelay = 0;
                        latencytuplecount = 0;
                    }
                }
            }
        }, 1,latencyTimerPeriod);// 设定指定的时间time,此处为10000毫秒

        //设置计时器没1s计算时间
        throughputTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //将最后结果输出到日志文件中
                outputCollector.emit(THROUGHPUT_STREAM_ID,new Values(thisTaskId,throughputCount,System.currentTimeMillis()));
                throughputCount = 0;
            }
        }, 10 ,throughputTimerPeriod);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void execute(Tuple input) {

    }

    protected void nextLatencyTuple(Tuple tuple){
        startTimeMills=tuple.getLongByField(START_TIME_MILLS);
        Long delay=System.currentTimeMillis()-startTimeMills;
        if(delay<0)
            return;

        synchronized (latencyLock) {
            latencytuplecount++;
            totalDelay += delay;
        }
    }

    protected void nextThroughputTuple(Tuple tuple){
        throughputCount++;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LATENCY_STREAM_ID,new Fields(TASK_ID,LATENCY_STREAM_AVGLATENCY,TIME_INFO));
        declarer.declareStream(THROUGHPUT_STREAM_ID,new Fields(TASK_ID,THROUGHPUT_STREAM_COUNT,TIME_INFO));
    }

    public Timer getLatencyTimer() {
        return latencyTimer;
    }

    public Timer getThroughputTimer() {
        return throughputTimer;
    }
}
