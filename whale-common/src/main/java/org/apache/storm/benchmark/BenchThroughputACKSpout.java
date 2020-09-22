package org.apache.storm.benchmark;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
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
 * Spout 利用ACK机制测试Storm吞吐量
 * locate org.apache.storm.benchmark
 * Created by master on 2019/10/15.
 */
public class BenchThroughputACKSpout<K, V> extends KafkaSpout<K, V> {
    private int throughputTimerPeriod;

    private Timer throughputTimer;
    private long throughputCount=0; //记录单位时间ACK的元组数量
    private int thisTaskId =0;


    public BenchThroughputACKSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig ,int throughputTimerPeriod) {
        super(kafkaSpoutConfig);
        this.throughputTimerPeriod= throughputTimerPeriod;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.thisTaskId=context.getThisTaskId();
        this.throughputTimer=new Timer();

        //设置计时器没1s计算时间
        throughputTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                collector.emit(THROUGHPUT_STREAM_ID,new Values(thisTaskId,throughputCount,System.currentTimeMillis()));
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
        super.ack(messageId);
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
        outputFieldsDeclarer.declareStream(THROUGHPUT_STREAM_ID,new Fields(TASK_ID,THROUGHPUT_STREAM_COUNT,TIME_INFO));
    }

    public Timer getThroughputTimer() {
        return throughputTimer;
    }
}

