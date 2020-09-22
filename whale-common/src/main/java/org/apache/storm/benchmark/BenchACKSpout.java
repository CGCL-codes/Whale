package org.apache.storm.benchmark;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutMessageId;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.storm.Constraints.*;

/**
 * locate org.apache.storm.benchmark
 * Spout 利用ACK机制测试Storm吞吐量和延迟
 * Created by master on 2019/10/15.
 */
public class BenchACKSpout<K, V> extends KafkaSpout<K, V> {
    private int latencyTimerPeriod;
    private int throughputTimerPeriod;

    private Timer latencyTimer;
    private Timer throughputTimer;
    private int thisTaskId =0;
    protected SpoutOutputCollector outputCollector;

    //latency
    private int latencytuplecount=0;
    private long totalDelay=0; //总和延
    private long startTimeMills;
    private Object latencyLock;

    //throughput
    private long throughputCount=0; //记录单位时间ACK的元组数量

    public BenchACKSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig, int latencyTimerPeriod, int throughputTimerPeriod) {
        super(kafkaSpoutConfig);
        this.latencyTimerPeriod = latencyTimerPeriod;
        this.throughputTimerPeriod = throughputTimerPeriod;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.thisTaskId = context.getThisTaskId();
        this.outputCollector = collector;
        this.latencyTimer = new Timer();
        this.throughputTimer = new Timer();
        this.latencyLock =new Object();

        //设置计时器没1s计算时间
        latencyTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(startTimeMills!=0 && latencytuplecount!=0){
                    synchronized (latencyLock) {
                        double avgDelay = ((double) totalDelay / (double) latencytuplecount);
                        avgDelay = (double) Math.round(avgDelay * 100) / 100;
                        outputCollector.emit(LATENCY_STREAM_ID, new Values(thisTaskId, avgDelay, System.currentTimeMillis()));
                        totalDelay = 0;
                        latencytuplecount = 0;
                    }
                }
            }
        }, 10,latencyTimerPeriod);// 设定指定的时间time,此处为1000毫秒

        //设置计时器没1s计算时间
        throughputTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                outputCollector.emit(THROUGHPUT_STREAM_ID,new Values(thisTaskId,throughputCount,System.currentTimeMillis()));
                throughputCount = 0;
            }
        }, 10,throughputTimerPeriod);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void nextTuple() {
        super.nextTuple();
    }

    @Override
    public void ack(Object messageId) {
        ackThroughputTuple();
        ackLatencyTuple(messageId);
        super.ack(messageId);
    }

    protected void ackLatencyTuple(Object messageId){
        final KafkaSpoutMessageId msgId = (KafkaSpoutMessageId) messageId;
        startTimeMills=latencyHashMap.get(msgId);
        long endTime=System.currentTimeMillis();
        long delay=endTime-startTimeMills;

        if(delay<0)
            return;

        synchronized (latencyLock) {
            latencytuplecount++;
            totalDelay += delay;
        }
    }

    protected void ackThroughputTuple(){
        throughputCount++;
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
        outputFieldsDeclarer.declareStream(LATENCY_STREAM_ID,new Fields(TASK_ID,LATENCY_STREAM_AVGLATENCY,TIME_INFO));
        outputFieldsDeclarer.declareStream(THROUGHPUT_STREAM_ID,new Fields(TASK_ID,THROUGHPUT_STREAM_COUNT,TIME_INFO));
    }

    public Timer getLatencyTimer() {
        return latencyTimer;
    }

    public int getThroughputTimerPeriod() {
        return throughputTimerPeriod;
    }

    public void setThroughputTimerPeriod(int throughputTimerPeriod) {
        this.throughputTimerPeriod = throughputTimerPeriod;
    }
}

