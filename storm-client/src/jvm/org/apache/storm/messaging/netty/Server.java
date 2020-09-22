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

import org.apache.storm.Config;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.utils.ObjectReader;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class  Server extends ConnectionWithStatus implements IStatefulObject, ISaslServer {

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    @SuppressWarnings("rawtypes")
    Map<String, Object> topoConf;
    int port;
    private final ConcurrentHashMap<String, AtomicInteger> messagesEnqueued = new ConcurrentHashMap<>();
    private final AtomicInteger messagesDequeued = new AtomicInteger(0);

    volatile ChannelGroup allChannels = new DefaultChannelGroup("storm-server");
    final ChannelFactory factory;
    final ServerBootstrap bootstrap;
 
    private volatile boolean closing = false;
    List<TaskMessage> closeMessage = Arrays.asList(new TaskMessage(-1, null));
    private KryoValuesSerializer _ser;
    private IConnectionCallback _cb = null; 
    private final int boundPort;

    @SuppressWarnings("rawtypes")
    Server(Map<String, Object> topoConf, int port) {
        this.topoConf = topoConf;
        this.port = port;
        _ser = new KryoValuesSerializer(topoConf);

        // Configure the server.
        int buffer_size = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        int backlog = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_SOCKET_BACKLOG), 500);
        int maxWorkers = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS));

        ThreadFactory bossFactory = new NettyRenameThreadFactory(netty_name() + "-boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory(netty_name() + "-worker");

        if (maxWorkers > 0) {
            factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                Executors.newCachedThreadPool(workerFactory), maxWorkers);
        } else {
            factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                Executors.newCachedThreadPool(workerFactory));
        }

        LOG.info("Create Netty Server " + netty_name() + ", buffer_size: " + buffer_size + ", maxWorkers: " + maxWorkers);

        bootstrap = new ServerBootstrap(factory);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.receiveBufferSize", buffer_size);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("backlog", backlog);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormServerPipelineFactory(this));

        // Bind and start to accept incoming connections.
        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        boundPort = ((InetSocketAddress)channel.getLocalAddress()).getPort();
        allChannels.add(channel);
    }
    
    private void addReceiveCount(String from, int amount) {
        //This is possibly lossy in the case where a value is deleted
        // because it has received no messages over the metrics collection
        // period and new messages are starting to come in.  This is
        // because I don't want the overhead of a synchronize just to have
        // the metric be absolutely perfect.
        AtomicInteger i = messagesEnqueued.get(from);
        if (i == null) {
            i = new AtomicInteger(amount);
            AtomicInteger prev = messagesEnqueued.putIfAbsent(from, i);
            if (prev != null) {
                prev.addAndGet(amount);
            }
        } else {
            i.addAndGet(amount);
        }
    }

    /**
     * enqueue a received message
     * @throws InterruptedException
     */
    protected void enqueue(List<WorkerMessage> msgs, String from) throws InterruptedException {
        if (null == msgs || msgs.size() == 0 || closing) {
            return;
        }
        addReceiveCount(from, msgs.size());
        if (_cb != null) {
            _cb.recv(msgs);
        }
    }

    @Override
    public void registerRecv(IConnectionCallback cb) {
        _cb = cb;
    }

    /**
     * register a newly created channel
     * @param channel newly created channel
     */
    protected void addChannel(Channel channel) {
        allChannels.add(channel);
    }

    /**
     * @param channel channel to close
     */
    public void closeChannel(Channel channel) {
        channel.close().awaitUninterruptibly();
        allChannels.remove(channel);
    }

    @Override
    public int getPort() {
        return boundPort;
    }
    
     /**
     * close all channels, and release resources
     */
    public synchronized void close() {
        if (allChannels != null) {
            allChannels.close().awaitUninterruptibly();
            factory.releaseExternalResources();
            allChannels = null;
        }
    }

    @Override
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
        try {
            MessageBatch mb = new MessageBatch(1);
            mb.add(new WorkerMessage(Arrays.asList(-1), _ser.serialize(Arrays.asList((Object)taskToLoad))));
            allChannels.write(mb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
        throw new RuntimeException("Server connection cannot get load");
    }

    @Override
    public void send(int task, byte[] message) {
        throw new UnsupportedOperationException("Server connection should not send any messages");
    }

    @Override
    public void send(WorkerMessage msgs) {
      throw new UnsupportedOperationException("Server connection should not send any messages");
    }

    public String netty_name() {
      return "Netty-server-localhost-" + port;
    }

    @Override
    public Status status() {
        if (closing) {
          return Status.Closed;
        }
        else if (!connectionEstablished(allChannels)) {
            return Status.Connecting;
        }
        else {
            return Status.Ready;
        }
    }

    private boolean connectionEstablished(Channel channel) {
      return channel != null && channel.isBound();
    }

    private boolean connectionEstablished(ChannelGroup allChannels) {
        boolean allEstablished = true;
        for (Channel channel : allChannels) {
            if (!(connectionEstablished(channel))) {
                allEstablished = false;
                break;
            }
        }
        return allEstablished;
    }

    public Object getState() {
        LOG.debug("Getting metrics for server on port {}", port);
        HashMap<String, Object> ret = new HashMap<>();
        ret.put("dequeuedMessages", messagesDequeued.getAndSet(0));
        HashMap<String, Integer> enqueued = new HashMap<String, Integer>();
        Iterator<Map.Entry<String, AtomicInteger>> it = messagesEnqueued.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, AtomicInteger> ent = it.next();
            //Yes we can delete something that is not 0 because of races, but that is OK for metrics
            AtomicInteger i = ent.getValue();
            if (i.get() == 0) {
                it.remove();
            } else {
                enqueued.put(ent.getKey(), i.getAndSet(0));
            }
        }
        ret.put("enqueued", enqueued);
        
        // Report messageSizes metric, if enabled (non-null).
        if (_cb instanceof IMetric) {
            Object metrics = ((IMetric) _cb).getValueAndReset();
            if (metrics instanceof Map) {
                ret.put("messageBytes", metrics);
            }
        }
        
        return ret;
    }

    /** Implementing IServer. **/
    public void channelConnected(Channel c) {
        addChannel(c);
    }

    public void received(Object message, String remote, Channel channel)  throws InterruptedException {
        List<WorkerMessage> messages = (List<WorkerMessage>) message;
        LOG.debug("Server received WorkerMessage : {}",messages);
        enqueue(messages, remote);
    }

    public String name() {
        return (String)topoConf.get(Config.TOPOLOGY_NAME);
    }

    public String secretKey() {
        return SaslUtils.getSecretKey(topoConf);
    }

    public void authenticated(Channel c) {
        return;
    }

    @Override
    public String toString() {
        return String.format("Netty server listening on port %s", port);
    }
}
