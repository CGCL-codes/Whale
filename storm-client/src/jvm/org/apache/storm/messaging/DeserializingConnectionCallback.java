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
package org.apache.storm.messaging;

import com.google.gson.Gson;
import org.apache.storm.Config;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.multicast.dynamicswitch.ControlConfiguration;
import org.apache.storm.multicast.dynamicswitch.ControlMsg;
import org.apache.storm.multicast.model.MulticastID;
import org.apache.storm.serialization.KryoTupleDeserializer;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A class that is called when a TaskMessage arrives.
 */
public class DeserializingConnectionCallback implements IConnectionCallback, IMetric {

    private static final Logger LOG = LoggerFactory.getLogger(DeserializingConnectionCallback.class);

    private final WorkerState.ILocalTransferCallback cb;
    private final WorkerState workerState;
    private final Map conf;
    private final GeneralTopologyContext context;

    private final ThreadLocal<KryoTupleDeserializer> _des =
        new ThreadLocal<KryoTupleDeserializer>() {
            @Override
            protected KryoTupleDeserializer initialValue() {
                return new KryoTupleDeserializer(conf, context);
            }
        };

    // Track serialized size of messages.
    private final boolean sizeMetricsEnabled;
    private final ConcurrentHashMap<String, AtomicLong> byteCounts = new ConcurrentHashMap<>();
    private long delay=0;

    private ExecutorService executorService;
    // componentID -> Map<StreamID, Map<DownstreamComponentId, GroupingStrategy>>
    private Map<String, Map<String, Map<String, Grouping>>> thisTargets = new HashMap<>();

    private Gson gson = new Gson();
    public DeserializingConnectionCallback(final Map conf, final GeneralTopologyContext context, WorkerState.ILocalTransferCallback callback , WorkerState workerState) {
        this.conf = conf;
        this.workerState = workerState;
        this.context = context;
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        cb = callback;
        sizeMetricsEnabled = ObjectReader.getBoolean(conf.get(Config.TOPOLOGY_SERIALIZED_MESSAGE_SIZE_METRICS), false);
        //PropertiesUtil.init("/storm-client-version-info.properties");
        //delay=Long.valueOf(PropertiesUtil.getProperties("serializationtime"));

        for (String componentId : this.context.getComponentIds()) {
            thisTargets.put(componentId, this.context.getTargets(componentId));
        }
    }

    //13.当有消息发送到Worker中时。Worker接收线程从接收队列中读取TaskMessage序列化后的数据，然后将其进行反序列化操作。最终得到带有消息头的AddressTuple。
    //然后调用回调函数的transfer方法。
    @Override
    public void recv(List<WorkerMessage> batch) {
        KryoTupleDeserializer des = _des.get();
        for(WorkerMessage workerMessage : batch){
            ArrayList<AddressedTuple> ret = new ArrayList<>();
            LOG.debug("the time of start deserializing : {}", System.currentTimeMillis());
            Tuple tuple = des.deserialize(workerMessage.message());
            //TimeUtils.waitForTimeMills(delay);

            // Whale-Multicast
            // 判断是否需要进行动态切换
            if(tuple.getStringByField("topic").equals("controlMsg")){
                ControlMsg controlMsg = gson.fromJson(tuple.getStringByField("value"), ControlMsg.class);
                ControlConfiguration controlConfiguration = controlMsg.getControlConfiguration().get(workerState.getWorkerId());
                Map<Integer, NodeInfo> taskToNodeInfo = workerState.getWorkerTopologyContext().getTaskToNodePort().get();
                //更新系统当前多播结构树
                conf.put("nonblockMultiTree", controlMsg.getGraph());
                workerState.setMulticastID(new MulticastID(controlConfiguration.getId(),controlConfiguration.getLayer(),controlConfiguration.getIndex()));
                if(controlConfiguration.getDisconnect() != null){
                    //多播控制配置信息 断开连接
                    workerState.closeConnection(taskToNodeInfo.get(Integer.valueOf(controlConfiguration.getDisconnect())));
                }else if(controlConfiguration.getReconnect() != null){
                    //多播控制配置信息 建立新的连接
                    workerState.reconnectConnection(taskToNodeInfo.get(Integer.valueOf(controlConfiguration.getReconnect())));
                }
                return;
            }

            // Whale-Multicast
            // 判断是否需要使用中继站的思想进行Tuple转发
            String sourceStreamId = tuple.getSourceStreamId();
            if(workerState.getMulticastGraph()!=null && (sourceStreamId.equals(conf.get(Config.TOPOLOGY_MULTICAST_SOURCE_STREMAID)) ||
                    sourceStreamId.equals(conf.get(Config.TOPOLOGY_MULTICAST_BROADCASTBOLT_STREMAID)))){
                String componentId = context.getTaskToComponent().get(workerMessage.tasks().get(0));
                if(thisTargets.get(componentId).containsKey(conf.get(Config.TOPOLOGY_MULTICAST_BROADCASTBOLT_STREMAID))){
                    workerState.transferForwardingWorkerMessage(workerMessage);
                    //workerState.transferForwardingWorkerMessage(workerMessage);
                    LOG.debug("In Multicast Forwarding ComponentId:{}, this Tragets:{}, sourceStreamId:{}", componentId, thisTargets.get(componentId), sourceStreamId);
                }
            }

            LOG.debug("recv WorkerMessage:{}",workerMessage);
            for(Integer taskid : workerMessage.tasks()){
                AddressedTuple addrTuple = new AddressedTuple(taskid, tuple);
                ret.add(addrTuple);
            }
            updateMetrics(tuple.getSourceTask(), workerMessage);
            LOG.debug("the time of end deserializing : {}", System.currentTimeMillis());
            cb.transfer(ret);
        }
    }

    /**
     * Returns serialized byte count traffic metrics.
     * @return Map of metric counts, or null if disabled
     */
    @Override
    public Object getValueAndReset() {
        if (!sizeMetricsEnabled) {
            return null;
        }
        HashMap<String, Long> outMap = new HashMap<>();
        for (Map.Entry<String, AtomicLong> ent : byteCounts.entrySet()) {
            AtomicLong count = ent.getValue();
            if (count.get() > 0) {
                outMap.put(ent.getKey(), count.getAndSet(0L));
            }
        }
        return outMap;
    }

    /**
     * Update serialized byte counts for each message.
     * @param sourceTaskId source task
     * @param message serialized message
     */
    protected void updateMetrics(int sourceTaskId, WorkerMessage message) {
        if (sizeMetricsEnabled) {
            List<Integer> dest = message.tasks();
            int len = message.message().length;
            String key = Integer.toString(sourceTaskId) + "-" + dest;
            byteCounts.computeIfAbsent(key, k -> new AtomicLong(0L)).addAndGet(len);
        }
    }

//    private class TransferForwardingTask implements Runnable{
//        private Tuple tuple;
//        private WorkerMessage workerMessage;
//
//        public TransferForwardingTask(Tuple tuple, WorkerMessage workerMessage) {
//            this.tuple = tuple;
//            this.workerMessage = workerMessage;
//        }
//
//        @Override
//        public void run() {
//
//        }
//    }
}
