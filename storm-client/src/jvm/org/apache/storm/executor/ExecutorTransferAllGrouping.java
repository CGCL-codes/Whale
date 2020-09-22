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
package org.apache.storm.executor;

import com.google.common.annotations.VisibleForTesting;
import com.lmax.disruptor.EventHandler;
import org.apache.storm.Config;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.MutableObject;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * 优化ExecutorTransferAllGrouping
 */
public class ExecutorTransferAllGrouping implements EventHandler, Callable {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorTransferAllGrouping.class);

    private final WorkerState workerData;
    private final DisruptorQueue batchTransferQueue;
    private final Map<String, Object> topoConf;
    private final KryoTupleSerializer serializer;
    private final MutableObject cachedEmit;
    private final boolean isDebug;

    public ExecutorTransferAllGrouping(WorkerState workerData, DisruptorQueue batchTransferQueue, Map<String, Object> topoConf) {
        this.workerData = workerData;
        this.batchTransferQueue = batchTransferQueue;
        this.topoConf = topoConf;
        this.serializer = new KryoTupleSerializer(topoConf, workerData.getWorkerTopologyContext());
        this.cachedEmit = new MutableObject(new ArrayList<>());
        this.isDebug = ObjectReader.getBoolean(topoConf.get(Config.TOPOLOGY_DEBUG), false);
    }

    //4.ExecutorTransfer将tuple添加目标task信息，将tuple封装成AddressedTuple。并将封装后的结果AddressedTuple publish到batchTransferQueue队列中。
    // batchTransferQueue也就是Executor的发送队列。
    public void transferBatchTuple(List<Integer> outTasks, Tuple tuple) {
        BatchTuple val = new BatchTuple(outTasks, tuple);
        if (isDebug) {
            LOG.info("transferBatchTuple BatchTuple : {}",val);
            LOG.info("TRANSFERRING tuple {}", val);
            LOG.info("the time of transferring BatchTuple : {}", System.currentTimeMillis());
        }
        batchTransferQueue.publish(val);
    }

    @VisibleForTesting
    public DisruptorQueue getBatchTransferQueue() {
        return this.batchTransferQueue;
    }

    //6.ExecutorTransfer的Call方法被调用。batchTransferQueue批量的消费消息
    @Override
    public Object call() throws Exception {
        batchTransferQueue.consumeBatchWhenAvailable(this);
        return 0L;
    }

    public String getName() {
        return batchTransferQueue.getName();
    }

    //7.相应事件，不断的批量消费batchTransferQueue中的AddressedTuple对象
    @Override
    public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
        ArrayList cachedEvents = (ArrayList) cachedEmit.getObject();
        cachedEvents.add(event);
        if (endOfBatch) {
            //8.调用WorkerState的transfer方法。对AddressedTuple进行序列化操作
            workerData.transferAllGrouping(serializer, cachedEvents);
            cachedEmit.setObject(new ArrayList<>());
        }
    }
}
