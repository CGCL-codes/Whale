/*
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
package org.apache.storm.daemon;

import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Acker implements IBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Acker.class);

    private static final long serialVersionUID = 4430906880683183091L;

    public static final String ACKER_COMPONENT_ID = "__acker";
    public static final String ACKER_INIT_STREAM_ID = "__ack_init";
    public static final String ACKER_ACK_STREAM_ID = "__ack_ack";
    public static final String ACKER_FAIL_STREAM_ID = "__ack_fail";
    public static final String ACKER_RESET_TIMEOUT_STREAM_ID = "__ack_reset_timeout";

    public static final int TIMEOUT_BUCKET_NUM = 3;

    private OutputCollector collector;
    private RotatingMap<Object, AckObject> pending;

    private static class AckObject {
        public long val = 0L;
        public long startTime = Time.currentTimeMillis();
        public int spoutTask = -1;
        public boolean failed = false;

        // val xor value
        public void updateAck(Long value) {
            val = Utils.bitXor(val, value);
        }
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.pending = new RotatingMap<>(TIMEOUT_BUCKET_NUM);
    }

    @Override
    public void execute(Tuple input) {
        /**
         *  Storm ACK 源码分析
         *  1.。Acker Bolt就是通过不断更新和检测跟踪值来判断该消息是否已经被完全成功处理的。RotatingMap主要用于消息的超时。AckerBolt不断的接受
         *  来自各个地方发送过来的Tuple，并且根据Tuple的getSourceStreamId 进行不同的逻辑操作
         */
        if (TupleUtils.isTick(input)) {
            /**
             * 系统预定义流，用于消息超时。Acker Bolt会对成员变量pending进行旋转操作，然后退出execute方法，该操作将pending中最早的一个桶中的数据删除掉，于是实现了。
             * 消息的超时。由于初始化RotatingMap时，未传入关于expire的回调方法，故该操作只是进行简单的删除。如果继续对已经删除掉的消息的Rootld进行Ack操作，就会创建新的
             * <RootId,跟踪值>对，但是由于数据已被删除过的原因，跟踪值基本上不会再回到零，所以Spout将永远也收不到它发送出去的这条消息的Ack。Spout会通过自有的超时机制，
             * 将这条消息标记为处理失败，然后调用Spout的失败函数来决定对失败消息进行重传还是忽略。这个操作的结果是去除处于僵死状态的消息跟踪。
              */
            Map<Object, AckObject> tmp = pending.rotate();
            LOG.debug("Number of timeout tuples:{}", tmp.size());
            return;
        }

        boolean resetTimeout = false;
        String streamId = input.getSourceStreamId();
        Object id = input.getValue(0);
        AckObject curr = pending.get(id);
        if (ACKER_INIT_STREAM_ID.equals(streamId)) {
            /**
             * Acker中AckObject初始化操作 Spout输入消息的模式为<RootId,RawAckValue,SpoutTaskId> AckerBolt会根据Rootld取出<RootId, AckValue> ,并进行更新操作以及设置
             * spoutTaskId
             */
            if (curr == null) {
                curr = new AckObject();
                pending.put(id, curr);
            }
            curr.updateAck(input.getLong(1));
            curr.spoutTask = input.getInteger(2);
        } else if (ACKER_ACK_STREAM_ID.equals(streamId)) {
            /**
             * ID：输人消息的模式为<RootId,AckValue〉，与原有AckValue进行异或操作并存储。
             */
            if (curr == null) {
                curr = new AckObject();
                pending.put(id, curr);
            }
            curr.updateAck(input.getLong(1));
        } else if (ACKER_FAIL_STREAM_ID.equals(streamId)) {
            // For the case that ack_fail message arrives before ack_init
            /**
             * Acker收到Bolt或者Spout发送过来的Fail消息。输入消息的模式为< RootId >。设置failed 为true, 表示消息的处理已经失败。
             */
            if (curr == null) {
                curr = new AckObject();
            }
            curr.failed = true;
            pending.put(id, curr);
        } else if (ACKER_RESET_TIMEOUT_STREAM_ID.equals(streamId)) {
            /**
             * AckerBolt 收到消息超时
             */
            resetTimeout = true;
            if (curr != null) {
                pending.put(id, curr);
            } //else if it has not been added yet, there is no reason time it out later on
        } else {
            LOG.warn("Unknown source stream {} from task-{}", streamId, input.getSourceTask());
            return;
        }

        int task = curr.spoutTask;
        if (curr != null && task >= 0
            && (curr.val == 0 || curr.failed || resetTimeout)) {
            Values tuple = new Values(id, getTimeDeltaMillis(curr.startTime));
            //若此时消息对应的跟踪值已经为零，那么Storm认为该消息以及所有衍生的消息都已被成功处理，这时会通过向ACK - STREAM流向Spout节点发送消息，模式为<RootId>
            if (curr.val == 0) {
                pending.remove(id);
                collector.emitDirect(task, ACKER_ACK_STREAM_ID, tuple);
            } else if (curr.failed) {
            //若此时消息被标记为失败，那么Storm会通过FAIL-STREAM流向Spout发送消息，模式为< RootId>
                pending.remove(id);
                collector.emitDirect(task, ACKER_FAIL_STREAM_ID, tuple);
            } else if(resetTimeout) {
            //若此时消息被标记为发送超时，那么Storm通过ACKER_RESET_TIMEOUT_STREAM流将tuple发送给Spout
                collector.emitDirect(task, ACKER_RESET_TIMEOUT_STREAM_ID, tuple);
            } else {
                throw new IllegalStateException("The checks are inconsistent we reach what should be unreachable code.");
            }
        }

        collector.ack(input);
    }

    @Override
    public void cleanup() {
        LOG.info("Acker: cleanup successfully");
    }

    private long getTimeDeltaMillis(long startTimeMillis) {
        return Time.currentTimeMillis() - startTimeMillis;
    }
}
