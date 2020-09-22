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
package org.apache.storm.messaging.netty;

import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.serialization.KryoValuesDeserializer;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.Map;

public class StormClientHandler extends SimpleChannelUpstreamHandler  {
    private static final Logger LOG = LoggerFactory.getLogger(StormClientHandler.class);
    private Client client;
    private KryoValuesDeserializer _des;

    StormClientHandler(Client client, Map<String, Object> conf) {
        this.client = client;
        _des = new KryoValuesDeserializer(conf);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        //examine the response message from server
        Object message = event.getMessage();
        if (message instanceof ControlMessage) {
            ControlMessage msg = (ControlMessage)message;
            if (msg==ControlMessage.FAILURE_RESPONSE) {
                LOG.info("failure response:{}", msg);
            }
        } else if (message instanceof List) {
            try {
                //This should be the metrics, and there should only be one of them
                List<WorkerMessage> list = (List<WorkerMessage>)message;
                if (list.size() < 1) throw new RuntimeException("Didn't see enough load metrics ("+client.getDstAddress()+") "+list);
                WorkerMessage workerMessage = ((List<WorkerMessage>)message).get(list.size() - 1);
                if (workerMessage.tasks().get(0) != -1) throw new RuntimeException("Metrics messages are sent to the system task ("+client.getDstAddress()+") "+workerMessage);
                List metrics = _des.deserialize(workerMessage.message());
                if (metrics.size() < 1) throw new RuntimeException("No metrics data in the metrics message ("+client.getDstAddress()+") "+metrics);
                if (!(metrics.get(0) instanceof Map)) throw new RuntimeException("The metrics did not have a map in the first slot ("+client.getDstAddress()+") "+metrics);
                client.setLoadMetrics((Map<Integer, Double>)metrics.get(0));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("Don't know how to handle a message of type "
                                       + message + " (" + client.getDstAddress() + ")");
        }
    }

    @Override
    public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        client.notifyInterestChanged(e.getChannel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) {
        Throwable cause = event.getCause();
        if (!(cause instanceof ConnectException)) {
            LOG.info("Connection to "+client.getDstAddress()+" failed:", cause);
        }
    }
}
