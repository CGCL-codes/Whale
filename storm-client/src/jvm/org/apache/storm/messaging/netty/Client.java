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
import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.StormBoundedExponentialBackoffRetry;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;

/**
 * A Netty client for sending task messages to a remote destination (Netty server).
 *
 * Implementation details:
 *
 * - Sending messages, i.e. writing to the channel, is performed asynchronously.
 * - 发送消息，即写入信道，是异步执行的。
 * - Messages are sent in batches to optimize for network throughput at the expense of network latency.  The message
 *   batch size is configurable.
 * - 消息以批量发送，以网络延迟为代价优化网络吞吐量。 消息批量大小是可配置的。
 * - Connecting and reconnecting are performed asynchronously.
 * - 连接和重新连接是异步执行的。
 *     - Note: The current implementation drops any messages that are being enqueued for sending if the connection to
 *       the remote destination is currently unavailable.
 * 如果与远程目标的连接当前不可用，则当前实现将丢弃正在排队发送的任何消息。
 */
public class Client extends ConnectionWithStatus implements IStatefulObject, ISaslClient {
    private static final long PENDING_MESSAGES_FLUSH_TIMEOUT_MS = 600000L;
    private static final long PENDING_MESSAGES_FLUSH_INTERVAL_MS = 1000L;

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final String PREFIX = "Netty-Client-";
    private static final long NO_DELAY_MS = 0L;
    private static final Timer timer = new Timer("Netty-ChannelAlive-Timer", true);

    private final Map<String, Object> topoConf;
    private final StormBoundedExponentialBackoffRetry retryPolicy;
    private final ClientBootstrap bootstrap;
    private final InetSocketAddress dstAddress;
    protected final String dstAddressPrefixedName;
    //The actual name of the host we are trying to connect to so that
    // when we remove ourselves from the connection cache there is no concern that
    // the resolved host name is different.
    private final String dstHost;
    private volatile Map<Integer, Double> serverLoad = null;

    /**
     * The channel used for all write operations from this client to the remote destination.
     */
    private final AtomicReference<Channel> channelRef = new AtomicReference<>();

    /**
     * Total number of connection attempts.
     */
    private final AtomicInteger totalConnectionAttempts = new AtomicInteger(0);

    /**
     * Number of connection attempts since the last disconnect.
     */
    private final AtomicInteger connectionAttempts = new AtomicInteger(0);

    /**
     * Number of messages successfully sent to the remote destination.
     */
    private final AtomicInteger messagesSent = new AtomicInteger(0);

    /**
     * Number of messages that could not be sent to the remote destination.
     */
    private final AtomicInteger messagesLost = new AtomicInteger(0);

    /**
     * Periodically checks for connected channel in order to avoid loss
     * of messages
     */
    private final long CHANNEL_ALIVE_INTERVAL_MS = 30000L;

    /**
     * Number of messages buffered in memory.
     */
    private final AtomicLong pendingMessages = new AtomicLong(0);

    /**
     * Whether the SASL channel is ready.
     */
    private final AtomicBoolean saslChannelReady = new AtomicBoolean(false);

    /**
     * This flag is set to true if and only if a client instance is being closed.
     */
    private volatile boolean closing = false;

    private final Context context;

    private final HashedWheelTimer scheduler;

    private final MessageBuffer batcher;

    private final Object writeLock = new Object();

    @SuppressWarnings("rawtypes")
    Client(Map<String, Object> topoConf, ChannelFactory factory, HashedWheelTimer scheduler, String host, int port, Context context) {
        this.topoConf = topoConf;
        closing = false;
        this.scheduler = scheduler;
        this.context = context;
        int bufferSize = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        // if SASL authentication is disabled, saslChannelReady is initialized as true; otherwise false
        saslChannelReady.set(!ObjectReader.getBoolean(topoConf.get(Config.STORM_MESSAGING_NETTY_AUTHENTICATION), false));
        LOG.info("creating Netty Client, connecting to {}:{}, bufferSize: {}", host, port, bufferSize);
        int messageBatchSize = ObjectReader.getInt(topoConf.get(Config.STORM_NETTY_MESSAGE_BATCH_SIZE), 262144);

        int maxReconnectionAttempts = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES));
        int minWaitMs = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        int maxWaitMs = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));
        retryPolicy = new StormBoundedExponentialBackoffRetry(minWaitMs, maxWaitMs, maxReconnectionAttempts);

        // Initiate connection to remote destination
        bootstrap = createClientBootstrap(factory, bufferSize, topoConf);

        dstHost = host;
        dstAddress = new InetSocketAddress(host, port);
        dstAddressPrefixedName = prefixedName(dstAddress);
        launchChannelAliveThread();
        scheduleConnect(NO_DELAY_MS);
        batcher = new MessageBuffer(messageBatchSize);
    }

    /**
     * This thread helps us to check for channel connection periodically.
     * This is performed just to know whether the destination address
     * is alive or attempts to refresh connections if not alive. This
     * solution is better than what we have now in case of a bad channel.
     */
    private void launchChannelAliveThread() {
        // netty TimerTask is already defined and hence a fully
        // qualified name
        timer.schedule(new java.util.TimerTask() {
            public void run() {
                try {
                    LOG.debug("running timer task, address {}", dstAddress);
                    if(closing) {
                        this.cancel();
                        return;
                    }
                    getConnectedChannel();
                } catch (Exception exp) {
                    LOG.error("channel connection error {}", exp);
                }
            }
        }, 0, CHANNEL_ALIVE_INTERVAL_MS);
    }

    private ClientBootstrap createClientBootstrap(ChannelFactory factory, int bufferSize, Map<String, Object> topoConf) {
        ClientBootstrap bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", bufferSize);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setPipelineFactory(new StormClientPipelineFactory(this, topoConf));
        return bootstrap;

    }

    private String prefixedName(InetSocketAddress dstAddress) {
        if (null != dstAddress) {
            return PREFIX + dstAddress.toString();
        }
        return "";
    }

    /**
     * Enqueue a task message to be sent to server
     */
    private void scheduleConnect(long delayMs) {
        scheduler.newTimeout(new Connect(dstAddress), delayMs, TimeUnit.MILLISECONDS);
    }

    private boolean reconnectingAllowed() {
        return !closing;
    }

    private boolean connectionEstablished(Channel channel) {
        // Because we are using TCP (which is a connection-oriented transport unlike UDP), a connection is only fully
        // established iff the channel is connected.  That is, a TCP-based channel must be in the CONNECTED state before
        // anything can be read or written to the channel.
        //
        // See:
        // - http://netty.io/3.9/api/org/jboss/netty/channel/ChannelEvent.html
        // - http://stackoverflow.com/questions/13356622/what-are-the-netty-channel-state-transitions
        return channel != null && channel.isConnected();
    }

    /**
     * Note:  Storm will check via this method whether a worker can be activated safely during the initial startup of a
     * topology.  The worker will only be activated once all of the its connections are ready.
     */
    @Override
    public Status status() {
        if (closing) {
            return Status.Closed;
        } else if (!connectionEstablished(channelRef.get())) {
            return Status.Connecting;
        } else {
            if (saslChannelReady.get()) {
                return Status.Ready;
            } else {
                return Status.Connecting; // need to wait until sasl channel is also ready
            }
        }
    }

    /**
     * Receiving messages is not supported by a client.
     *
     * @throws java.lang.UnsupportedOperationException whenever this method is being called.
     */
    @Override
    public void registerRecv(IConnectionCallback cb) {
        throw new UnsupportedOperationException("Client connection should not receive any messages");
    }

    @Override
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
        throw new RuntimeException("Client connection should not send load metrics");
    }

    @Override
    public void send(int taskId, byte[] payload) {
        WorkerMessage workerMessage=new WorkerMessage(Arrays.asList(taskId),payload);
        send(workerMessage);
    }

    ////////////////////////////////////优化transferAllGrouping/////////////////////////
    /**
     * Enqueue task messages to be sent to the remote destination (cf. `host` and `port`).
     */
    @Override
    public void send(WorkerMessage msgs) {
        LOG.debug("Client send msg : {}",msgs);
        if (closing) {
            int numMessages = msgs.tasks().size();
            LOG.error("discarding {} messages because the Netty client to {} is being closed", numMessages,
                    dstAddressPrefixedName);
            return;
        }

        if (!hasMessages(msgs)) {
            return;
        }

        Channel channel = getConnectedChannel();
        if (channel == null) {
            /*
             * Connection is unavailable. We will drop pending messages and let at-least-once message replay kick in.
             *
             * Another option would be to buffer the messages in memory.  But this option has the risk of causing OOM errors,
             * especially for topologies that disable message acking because we don't know whether the connection recovery will
             * succeed  or not, and how long the recovery will take.
             */
            dropMessages(msgs);
            return;
        }

        synchronized (writeLock) {
            MessageBatch full = batcher.add(msgs);
            if(full != null){
                flushMessages(channel, full);
            }
        }

        if(channel.isWritable()){
            synchronized (writeLock) {
                // Netty's internal buffer is not full and we still have message left in the buffer.
                // We should write the unfilled MessageBatch immediately to reduce latency
                MessageBatch batch = batcher.drain();
                if(batch != null) {
                    flushMessages(channel, batch);
                }
            }
        } else {
            // Channel's buffer is full, meaning that we have time to wait other messages to arrive, and create a bigger
            // batch. This yields better throughput.
            // We0 can rely on `notifyInterestChanged` to push these messages as soon as there is spece in Netty's buffer
            // because we know `Channel.isWritable` was false after the messages were already in the buffer.
        }

    }
    ////////////////////////////////////优化transferAllGrouping/////////////////////////////////

    private Channel getConnectedChannel() {
        Channel channel = channelRef.get();
        if (connectionEstablished(channel)) {
            return channel;
        } else {
            // Closing the channel and reconnecting should be done before handling the messages.
            boolean reconnectScheduled = closeChannelAndReconnect(channel);
            if (reconnectScheduled) {
                // Log the connection error only once
                LOG.error("connection to {} is unavailable", dstAddressPrefixedName);
            }
            return null;
        }
    }

    public InetSocketAddress getDstAddress() {
        return dstAddress;
    }

    private boolean hasMessages(WorkerMessage workerMessage) {
        return workerMessage != null && workerMessage.tasks().size()!=0;
    }

    private void dropMessages(WorkerMessage msgs) {
        // We consume the iterator by traversing and thus "emptying" it.
        int msgCount = iteratorSize(msgs);
        messagesLost.getAndAdd(msgCount);
    }

    private int iteratorSize(WorkerMessage msgs) {
        return msgs.tasks().size();
    }

    /**
     * Asynchronously writes the message batch to the channel.
     *
     * If the write operation fails, then we will close the channel and trigger a reconnect.
     */
    private void flushMessages(Channel channel, final MessageBatch batch) {
        if (null == batch || batch.isEmpty()) {
            return;
        }

        final int numMessages = batch.size();
        LOG.debug("writing {} messages to channel {}", batch.size(), channel.toString());
        pendingMessages.addAndGet(numMessages);

        ChannelFuture future = channel.write(batch);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) throws Exception {
                pendingMessages.addAndGet(0 - numMessages);
                if (future.isSuccess()) {
                    LOG.debug("sent {} messages to {}", numMessages, dstAddressPrefixedName);
                    messagesSent.getAndAdd(batch.size());
                } else {
                    LOG.error("failed to send {} messages to {}: {}", numMessages, dstAddressPrefixedName,
                            future.getCause());
                    closeChannelAndReconnect(future.getChannel());
                    messagesLost.getAndAdd(numMessages);
                }
            }
        });
    }

    /**
     * Schedule a reconnect if we closed a non-null channel, and acquired the right to
     * provide a replacement by successfully setting a null to the channel field
     * @param channel
     * @return if the call scheduled a re-connect task
     */
    private boolean closeChannelAndReconnect(Channel channel) {
        if (channel != null) {
            channel.close();
            if (channelRef.compareAndSet(channel, null)) {
                scheduleConnect(NO_DELAY_MS);
                return true;
            }
        }
        return false;
    }

    @Override
    public int getPort() {
        return dstAddress.getPort();
    }
    
    /**
     * Gracefully close this client.
     */
    @Override
    public void close() {
        if (!closing) {
            LOG.info("closing Netty Client {}", dstAddressPrefixedName);
            context.removeClient(dstHost, dstAddress.getPort());
            // Set closing to true to prevent any further reconnection attempts.
            closing = true;
            waitForPendingMessagesToBeSent();
            closeChannel();
        }
    }

    private void waitForPendingMessagesToBeSent() {
        LOG.info("waiting up to {} ms to send {} pending messages to {}",
                PENDING_MESSAGES_FLUSH_TIMEOUT_MS, pendingMessages.get(), dstAddressPrefixedName);
        long totalPendingMsgs = pendingMessages.get();
        long startMs = System.currentTimeMillis();
        while (pendingMessages.get() != 0) {
            try {
                long deltaMs = System.currentTimeMillis() - startMs;
                if (deltaMs > PENDING_MESSAGES_FLUSH_TIMEOUT_MS) {
                    LOG.error("failed to send all pending messages to {} within timeout, {} of {} messages were not " +
                            "sent", dstAddressPrefixedName, pendingMessages.get(), totalPendingMsgs);
                    break;
                }
                Thread.sleep(PENDING_MESSAGES_FLUSH_INTERVAL_MS);
            } catch (InterruptedException e) {
                break;
            }
        }

    }

    private void closeChannel() {
        Channel channel = channelRef.get();
        if (channel != null) {
            channel.close();
            LOG.debug("channel to {} closed", dstAddressPrefixedName);
        }
    }

    void setLoadMetrics(Map<Integer, Double> taskToLoad) {
        this.serverLoad = taskToLoad;
    }

    @Override
    public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
        Map<Integer, Double> loadCache = serverLoad;
        Map<Integer, Load> ret = new HashMap<Integer, Load>();
        if (loadCache != null) {
            double clientLoad = Math.min(pendingMessages.get(), 1024)/1024.0;
            for (Integer task : tasks) {
                Double found = loadCache.get(task);
                if (found != null) {
                    ret.put(task, new Load(true, found, clientLoad));
                }
            }
        }
        return ret;
    }

    @Override
    public Object getState() {
        LOG.debug("Getting metrics for client connection to {}", dstAddressPrefixedName);
        HashMap<String, Object> ret = new HashMap<String, Object>();
        ret.put("reconnects", totalConnectionAttempts.getAndSet(0));
        ret.put("sent", messagesSent.getAndSet(0));
        ret.put("pending", pendingMessages.get());
        ret.put("lostOnSend", messagesLost.getAndSet(0));
        ret.put("dest", dstAddress.toString());
        String src = srcAddressName();
        if (src != null) {
            ret.put("src", src);
        }
        return ret;
    }

    public Map getConfig() {
        return topoConf;
    }

    /** ISaslClient interface **/
    public void channelConnected(Channel channel) {
//        setChannel(channel);
    }

    public void channelReady() {
        saslChannelReady.set(true);
    }

    public String name() {
        return (String)topoConf.get(Config.TOPOLOGY_NAME);
    }

    public String secretKey() {
        return SaslUtils.getSecretKey(topoConf);
    }
    /** end **/

    private String srcAddressName() {
        String name = null;
        Channel channel = channelRef.get();
        if (channel != null) {
            SocketAddress address = channel.getLocalAddress();
            if (address != null) {
                name = address.toString();
            }
        }
        return name;
    }

    @Override
    public String toString() {
        return String.format("Netty client for connecting to %s", dstAddressPrefixedName);
    }

    /**
     * Called by Netty thread on change in channel interest
     * @param channel
     */
    public void notifyInterestChanged(Channel channel) {
        if(channel.isWritable()){
            synchronized (writeLock) {
                // Channel is writable again, write if there are any messages pending
                MessageBatch pending = batcher.drain();
                flushMessages(channel, pending);
            }
        }
    }

    /**
     * Asynchronously establishes a Netty connection to the remote address
     * This task runs on a single thread shared among all clients, and thus
     * should not perform operations that block.
     */
    private class Connect implements TimerTask {

        private final InetSocketAddress address;

        public Connect(InetSocketAddress address) {
            this.address = address;
        }

        private void reschedule(Throwable t) {
            String baseMsg = String.format("connection attempt %s to %s failed", connectionAttempts,
                    dstAddressPrefixedName);
            String failureMsg = (t == null) ? baseMsg : baseMsg + ": " + t.toString();
            LOG.error(failureMsg);
            long nextDelayMs = retryPolicy.getSleepTimeMs(connectionAttempts.get(), 0);
            scheduleConnect(nextDelayMs);
        }


        @Override
        public void run(Timeout timeout) throws Exception {
            if (reconnectingAllowed()) {
                final int connectionAttempt = connectionAttempts.getAndIncrement();
                totalConnectionAttempts.getAndIncrement();

                LOG.debug("connecting to {} [attempt {}]", address.toString(), connectionAttempt);
                ChannelFuture future = bootstrap.connect(address);
                future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        // This call returns immediately
                        Channel newChannel = future.getChannel();

                        if (future.isSuccess() && connectionEstablished(newChannel)) {
                            boolean setChannel = channelRef.compareAndSet(null, newChannel);
                            checkState(setChannel);
                            LOG.debug("successfully connected to {}, {} [attempt {}]", address.toString(), newChannel.toString(),
                                    connectionAttempt);
                            if (messagesLost.get() > 0) {
                                LOG.warn("Re-connection to {} was successful but {} messages has been lost so far", address.toString(), messagesLost.get());
                            }
                        } else {
                            Throwable cause = future.getCause();
                            reschedule(cause);
                            if (newChannel != null) {
                                newChannel.close();
                            }
                        }
                    }
                });
            } else {
                close();
                throw new RuntimeException("Giving up to scheduleConnect to " + dstAddressPrefixedName + " after " +
                        connectionAttempts + " failed attempts. " + messagesLost.get() + " messages were lost");

            }
        }
    }
}
