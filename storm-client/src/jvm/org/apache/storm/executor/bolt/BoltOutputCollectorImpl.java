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
package org.apache.storm.executor.bolt;

import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.Task;
import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltFailInfo;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Storm ACK 源码分析
 * 4.Bolt在发送消息时，系统需要继续对其进行跟踪，这些由Bolt新发送的消息对应于从Spout收到消息的衍生消息。Bolt使用bolt -emit函数来发送消息
 */
public class BoltOutputCollectorImpl implements IOutputCollector {

    private static final Logger LOG = LoggerFactory.getLogger(BoltOutputCollectorImpl.class);

    private final BoltExecutor executor;
    private final Task taskData;
    private final int taskId;
    private final Random random;
    private final boolean isEventLoggers;
    private final boolean isDebug;

    public BoltOutputCollectorImpl(BoltExecutor executor, Task taskData, int taskId, Random random,
                                   boolean isEventLoggers, boolean isDebug) {
        this.executor = executor;
        this.taskData = taskData;
        this.taskId = taskId;
        this.random = random;
        this.isEventLoggers = isEventLoggers;
        this.isDebug = isDebug;
    }

    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        return boltEmit(streamId, anchors, tuple, null);
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        boltEmit(streamId, anchors, tuple, taskId);
    }

    private List<Integer> boltEmit(String streamId, Collection<Tuple> anchors, List<Object> values, Integer targetTaskId) {
        List<Integer> outTasks;
        if (targetTaskId != null) {
            outTasks = taskData.getOutgoingTasks(targetTaskId, streamId, values);
        } else {
            outTasks = taskData.getOutgoingTasks(streamId, values);
        }

        /**
         * Acker源码分析
         * bolt-emit函数的传入参数为消息标记（ anchors ),它对应于该消息的父节点消息。为了保证Ack系统正常工作，用户需要明确其产生的消息是由哪些消息衍生的。
         */

        ////////////////////////////////////优化BoltOutputCollector/////////////////////////
        Map<Long, Long> anchorsToIds = new HashMap<>();
        if (anchors != null) {
            for (Tuple a : anchors) {
                Set<Long> rootIds = a.getMessageId().getAnchorsToIds().keySet();
                if (rootIds.size() > 0) {
                    long edgeId = MessageId.generateId(random);
                    ((TupleImpl) a).updateAckVal(edgeId);
                    for (Long root_id : rootIds) {
                        putXor(anchorsToIds, root_id, edgeId);
                    }
                }
            }
        }
        MessageId msgId = MessageId.makeId(anchorsToIds);
        TupleImpl tupleExt = new TupleImpl(executor.getWorkerTopologyContext(), values, taskId, streamId, msgId);
        executor.getExecutorTransferAllGrouping().transferBatchTuple(outTasks,tupleExt);
        ////////////////////////////////////优化BoltOutputCollector/////////////////////////

        if (isEventLoggers) {
            executor.sendToEventLogger(executor, taskData, values, executor.getComponentId(), null, random);
        }
        return outTasks;
    }

    @Override
    public void ack(Tuple input) {
        long ackValue = ((TupleImpl) input).getAckVal();
        Map<Long, Long> anchorsToIds = input.getMessageId().getAnchorsToIds();
        for (Map.Entry<Long, Long> entry : anchorsToIds.entrySet()) {
            executor.sendUnanchored(taskData, Acker.ACKER_ACK_STREAM_ID,
                    new Values(entry.getKey(), Utils.bitXor(entry.getValue(), ackValue)),
                    executor.getExecutorTransferAllGrouping());
        }
        long delta = tupleTimeDelta((TupleImpl) input);
        if (isDebug) {
            LOG.info("BOLT ack TASK: {} TIME: {} TUPLE: {}", taskId, delta, input);
        }
        BoltAckInfo boltAckInfo = new BoltAckInfo(input, taskId, delta);
        boltAckInfo.applyOn(taskData.getUserContext());
        if (delta >= 0) {
            ((BoltExecutorStats) executor.getStats()).boltAckedTuple(
                    input.getSourceComponent(), input.getSourceStreamId(), delta);
        }
    }

    @Override
    public void fail(Tuple input) {
        Set<Long> roots = input.getMessageId().getAnchors();
        for (Long root : roots) {
            executor.sendUnanchored(taskData, Acker.ACKER_FAIL_STREAM_ID,
                    new Values(root), executor.getExecutorTransferAllGrouping());
        }
        long delta = tupleTimeDelta((TupleImpl) input);
        if (isDebug) {
            LOG.info("BOLT fail TASK: {} TIME: {} TUPLE: {}", taskId, delta, input);
        }
        BoltFailInfo boltFailInfo = new BoltFailInfo(input, taskId, delta);
        boltFailInfo.applyOn(taskData.getUserContext());
        if (delta >= 0) {
            ((BoltExecutorStats) executor.getStats()).boltFailedTuple(
                    input.getSourceComponent(), input.getSourceStreamId(), delta);
        }
    }

    @Override
    public void resetTimeout(Tuple input) {
        Set<Long> roots = input.getMessageId().getAnchors();
        for (Long root : roots) {
            executor.sendUnanchored(taskData, Acker.ACKER_RESET_TIMEOUT_STREAM_ID,

                    new Values(root), executor.getExecutorTransferAllGrouping());
        }
    }

    @Override
    public void reportError(Throwable error) {
        executor.getErrorReportingMetrics().incrReportedErrorCount();
        executor.getReportError().report(error);
    }

    private long tupleTimeDelta(TupleImpl tuple) {
        Long ms = tuple.getProcessSampleStartTime();
        if (ms != null) {
            return Time.deltaMs(ms);
        }
        return -1;
    }

    /**
     * 根据key id 持续更新pending
     *
     * @param pending
     * @param key
     * @param id
     */
    private void putXor(Map<Long, Long> pending, Long key, Long id) {
        Long curr = pending.get(key);
        if (curr == null) {
            curr = 0l;
        }
        pending.put(key, Utils.bitXor(curr, id));
    }
}
