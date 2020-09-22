/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.executor.spout;

import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.Task;
import org.apache.storm.executor.TupleInfo;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MutableLong;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class WhaleSpoutOutputCollectorImpl implements ISpoutOutputCollector {

    private final SpoutExecutor executor;
    private final Task taskData;
    private final int taskId;
    private final MutableLong emittedCount;
    private final boolean hasAckers;
    private final Random random;
    private final Boolean isEventLoggers;
    private final Boolean isDebug;
    private final RotatingMap<Long, TupleInfo> pending;
    public static final Logger LOG = LoggerFactory.getLogger(WhaleSpoutOutputCollectorImpl.class);

    private Timer streamMonitor;
    private long  inputTupleCount = 0; //记录单位时间输入的元组数量
    private long  inputRate = 0; //记录输入到流多播系统中的平均输入速率
    private int streamMonitorPeriod = 1000; //streamMonitor的监控周期为1s
    @SuppressWarnings("unused")
    public WhaleSpoutOutputCollectorImpl(ISpout spout, SpoutExecutor executor, Task taskData, int taskId,
                                         MutableLong emittedCount, boolean hasAckers, Random random,
                                         Boolean isEventLoggers, Boolean isDebug, RotatingMap<Long, TupleInfo> pending) {
        this.executor = executor;
        this.taskData = taskData;
        this.taskId = taskId;
        this.emittedCount = emittedCount;
        this.hasAckers = hasAckers;
        this.random = random;
        this.isEventLoggers = isEventLoggers;
        this.isDebug = isDebug;
        this.pending = pending;

        //whale-multicast
        this.streamMonitor = new Timer();
        //设置计时器没1s计算时间
        this.streamMonitor.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                inputTupleCount = 0;
                inputRate = inputTupleCount;
            }
        }, 10,streamMonitorPeriod);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        inputTupleCount++;
        return sendSpoutMsg(streamId, tuple, messageId, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        sendSpoutMsg(streamId, tuple, messageId, taskId);
    }

    @Override
    public long getPendingCount() {
        return pending.size();
    }

    @Override
    public void reportError(Throwable error) {
        executor.getErrorReportingMetrics().incrReportedErrorCount();
        executor.getReportError().report(error);
    }

    /**
     * 1.首先 Spout调用sendSpoutMsg() 发送一个tuple到下游bolt
     * @param stream
     * @param values
     * @param messageId
     * @param outTaskId
     * @return
     */
    private List<Integer> sendSpoutMsg(String stream, List<Object> values, Object messageId, Integer outTaskId) {
        emittedCount.increment();

        List<Integer> outTasks;
        if (outTaskId != null) {
            outTasks = taskData.getOutgoingTasks(outTaskId, stream, values);
        } else {
            outTasks = taskData.getOutgoingTasks(stream, values);
        }

        List<Long> ackSeq = new ArrayList<>();
        boolean needAck = (messageId != null) && hasAckers;

        long rootId = MessageId.generateId(random);
        if(isDebug)
            LOG.info("the time of copying start: {}", System.currentTimeMillis());

        /**
         * Storm ACK 源码分析
         * 2.Storm中每条发送出去的消息都会对应一个随机的消息ID,并且这个long类型的消息ID将保存到MessageId这个对象中去。
         *  MessageId随着TupleImpl发送到下游相应的Bolt中去。若系统中含有Acker Bolt, 并且Spout在发送消息时指定了Messageld, Storm将对这条消息进行跟踪，并为其生成一条Rootld，
         *  然后为发送到每一个Task上面的消息也生成一个消息ID。消息ID是通过调用Messageld的generateld方法来产生的，为一个长整型随机数。
         */
        ////////////////////////////////////优化SpoutOutputCollector/////////////////////////
        MessageId msgId;
        if (needAck) {
            long as = MessageId.generateId(random);
            msgId = MessageId.makeRootId(rootId, as);
            ackSeq.add(as);
        } else {
            msgId = MessageId.makeUnanchored();
        }

        TupleImpl tuple = new TupleImpl(executor.getWorkerTopologyContext(), values, this.taskId, stream, msgId);
        executor.getExecutorTransferAllGrouping().transferBatchTuple(outTasks,tuple);
        ////////////////////////////////////优化SpoutOutputCollector/////////////////////////

        if(isDebug){
            LOG.info("the time of copying end: {}", System.currentTimeMillis());
            LOG.info("the time of transferring tuple : {}", System.currentTimeMillis());
        }

        if (isEventLoggers) {
            executor.sendToEventLogger(executor, taskData, values, executor.getComponentId(), messageId, random);
        }

        /**
         * Storm ACK 源码分析
         * 3.首先Spout 发送一个锚定(anchored) ACKER_INIT_STREAM_ID 的消息给Acker Bolt。Acker Bolt将这个tuple的rootId进行保存下来，并且保存
         *  相应的_outAckVal(ACK值)以及Spout的TaskId。这个ACK值还是当前spout发送这个rootId对应的Tuples的异或值。
         */
        boolean sample = false;
        try {
            sample = executor.getSampler().call();
        } catch (Exception ignored) {
        }
        if (needAck) {
            /**
             * 将<RootId,元数据〉存储在RotatingMap中。消息元数据中含有Spout的Taskld、Messageld,Streamld以及消息的内容。存储原始的消息内容可以方便以后的失败重传，
             * ACKER_INIT_STREAM_ID 发送__ack__init流将 ackInitTuple发送给AckerBolt
             */
            TupleInfo info = new TupleInfo();
            info.setTaskId(this.taskId);
            info.setStream(stream);
            info.setMessageId(messageId);
            if (isDebug) {
                info.setValues(values);
            }
            if (sample) {
                info.setTimestamp(System.currentTimeMillis());
            }

            pending.put(rootId, info);
            List<Object> ackInitTuple = new Values(rootId, Utils.bitXorVals(ackSeq), this.taskId);
            executor.sendUnanchored(taskData, Acker.ACKER_INIT_STREAM_ID, ackInitTuple, executor.getExecutorTransferAllGrouping());
        } else if (messageId != null) {
            TupleInfo info = new TupleInfo();
            info.setStream(stream);
            info.setValues(values);
            info.setMessageId(messageId);
            info.setTimestamp(0);
            Long timeDelta = sample ? 0L : null;
            info.setId("0:");
            //针对Spout发送消息时带有Messageld但系统中并没有Acker Bolt情况的一种特殊处理。此时，Storm将直接调用Spout的Ack方法，系统不对消息进行跟踪。
            executor.ackSpoutMsg(executor, taskData, timeDelta, info);
        }

        return outTasks;
    }

    /**
     * 获取摄入到系统中的平均输入速率
     * @return
     */
    @Override
    public long getInputRate() {
        return inputRate;
    }
}
