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
 * Bolt 测试Storm延迟
 * locate org.apache.storm.benchmark
 * Created by master on 2019/10/14.
 */
public class BenchLatencyBolt extends BaseRichBolt {
    private int latencyTimerPeriod;

    private Timer latencyTimer;
    private int thisTaskId = 0;
    protected OutputCollector outputCollector;

    //latency
    private int latencytuplecount=0;
    private long totalDelay=0; //总和延迟
    private long startTimeMills;
    private Object latencyLock;

    public BenchLatencyBolt(int latencyTimerPeriod) {
        this.latencyTimerPeriod = latencyTimerPeriod;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.thisTaskId = context.getThisTaskId();
        this.outputCollector=collector;
        this.latencyTimer = new Timer();
        this.latencyLock =new Object();

        //设置计时器没1s计算时间
        latencyTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(startTimeMills!=0 && latencytuplecount!=0){
                    synchronized (latencyLock) {
                        double avgDelay = ((double) totalDelay / (double) latencytuplecount);
                        avgDelay = (double) Math.round(avgDelay * 100) / 100;
                        collector.emit(LATENCY_STREAM_ID, new Values(thisTaskId, avgDelay, System.currentTimeMillis()));
                        totalDelay = 0;
                        latencytuplecount = 0;
                    }
                }
            }
        }, 10,latencyTimerPeriod);// 设定指定的时间time,此处为10000毫秒
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

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(LATENCY_STREAM_ID,new Fields(TASK_ID,LATENCY_STREAM_AVGLATENCY,TIME_INFO));
    }

    public Timer getLatencyTimer() {
        return latencyTimer;
    }
}
