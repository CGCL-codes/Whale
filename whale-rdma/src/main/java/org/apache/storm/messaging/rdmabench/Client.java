package org.apache.storm.messaging.rdmabench;

import com.basic.rdmachannel.RDMANodeContainer;
import com.basic.rdmachannel.channel.RdmaChannel;
import com.basic.rdmachannel.channel.RdmaCompletionListener;
import com.basic.rdmachannel.channel.RdmaNode;
import com.basic.rdmachannel.mr.RdmaBuffer;
import org.apache.storm.Config;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.ClusterAddressHost;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.StormBoundedExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * locate org.apache.storm.rdma
 * Created by mastertj on 2018/9/11.
 */
public class Client extends ConnectionWithStatus implements IStatefulObject {
    private static final long PENDING_MESSAGES_FLUSH_TIMEOUT_MS = 600000L;
    private static final long PENDING_MESSAGES_FLUSH_INTERVAL_MS = 1000L;
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionWithStatus.class);

    private static final String PREFIX = "RDMA-Client-";
    private static final Timer timer = new Timer("RDMA-ChannelAlive-Timer", true);
    private static Timer flushMessageTimer = new Timer("RDMA-FlushMessage-Timer", true);
    private static final long NO_DELAY_MS = 0L;

    private final StormBoundedExponentialBackoffRetry retryPolicy;

    /**
     * The channel used for all write operations from this client to the remote destination.
     */
    private final AtomicReference<RdmaChannel> channelRef = new AtomicReference<>();

    /**
     * This flag is set to true if and only if a client instance is being closed.
     */
    private volatile boolean closing = false;

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

    private final InetSocketAddress dstAddress;

    private volatile Map<Integer, Double> serverLoad = null;

    protected final String dstAddressPrefixedName;
    //The actual name of the host we are trying to connect to so that
    // when we remove ourselves from the connection cache there is no concern that
    // the resolved host name is different.
    private final String dstHost;

    private final Context context;

    private final Object writeLock = new Object();
    private final Map<String, Object> topoConf;

    //RDMANode and RDMA Channel
    private RdmaNode rdmaNode;

    private final MessageBuffer batcher;

    private long sendTimeLimit;

    //private HashedWheelTimer scheduler;

    private final ExecutorService connectThreadPool = Executors.newCachedThreadPool();

    private FlushMessageTimerTask flushMessageTimerTask;
    //////////////////////////////////////RDMA///////////////////////////////////

    private Timer flushMessageTimesTimer=new Timer();
    private BufferedWriter bufferedWriter2;

    Client(Map<String, Object> topoConf, String host, int port, Context context) throws Exception {
        this.topoConf = topoConf;
        this.context = context;
        this.rdmaNode = RDMANodeContainer.getInstance();

        closing = false;

        this.dstHost= ClusterAddressHost.resovelAddressHost(host);
        LOG.info("creating Netty Client, connecting to {}:{}", this.dstHost, this.dstHost);

        dstAddress = new InetSocketAddress(dstHost, port);
        dstAddressPrefixedName = prefixedName(dstAddress);

        //scheduler = new HashedWheelTimer(new NettyRenameThreadFactory("client-schedule-service"));

        int messageBatchSize = ObjectReader.getInt(topoConf.get(Config.STORM_RDMA_MESSAGE_BATCH_SIZE), 262144);
        this.sendTimeLimit=ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_RDMA_SEND_LIMIT_TIME), 800);

        int maxReconnectionAttempts = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES));
        int minWaitMs = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        int maxWaitMs = ObjectReader.getInt(topoConf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));
        retryPolicy = new StormBoundedExponentialBackoffRetry(minWaitMs, maxWaitMs, maxReconnectionAttempts);

        batcher = new MessageBuffer(messageBatchSize);

        launchChannelAliveThread();
        scheduleConnect(NO_DELAY_MS);

        scheduleFlushMessage();

        BufferedWriter bufferedWriter1=new BufferedWriter(new FileWriter("/whale/rdmabench/flushMessagesTimes-"+host+"-"+port+".out"));
        bufferedWriter2=new BufferedWriter(new FileWriter("/whale/rdmabench/rdmaSendLatency-"+host+"-"+port+".out"));
        //??????????????????1s????????????
        flushMessageTimesTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //???????????????????????????????????????
                try {
                    bufferedWriter1.write(flushMessagesTimes+"\n");
                    bufferedWriter1.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                flushMessagesTimes = 0;
            }
        }, 0,100);// ?????????????????????time,?????????1000??????
    }

    public String resovelAddressHost(String host){
        String dstHost="";
        switch (host){
            case "node2":
                dstHost="10.10.0.2";
                break;
            case "node3":
                dstHost="10.10.0.3";
                break;

            case "node7":
                dstHost="10.10.0.7";
                break;
            case "node12":
                dstHost="10.10.0.12";
                break;

            case "node18":
                dstHost="10.10.0.18";
                break;
            case "node19":
                dstHost="10.10.0.19";
                break;

            case "node23":
                dstHost="10.10.0.23";
                break;
            case "node24":
                dstHost="10.10.0.24";
                break;
            case "node25":
                dstHost="10.10.0.25";
                break;
            case "node26":
                dstHost="10.10.0.26";
                break;
            case "node27":
                dstHost="10.10.0.27";
                break;
            case "node28":
                dstHost="10.10.0.28";
                break;

            case "node30":
                dstHost="10.10.0.30";
                break;
            case "node32":
                dstHost="10.10.0.32";
                break;
            case "node34":
                dstHost="10.10.0.34";
                break;
            case "node37":
                dstHost="10.10.0.37";
                break;

            case "node61":
                dstHost="10.10.0.61";
                break;
            case "node62":
                dstHost="10.10.0.62";
                break;
            case "node63":
                dstHost="10.10.0.63";
                break;

            case "node84":
                dstHost="10.10.0.84";
                break;
            case "node85":
                dstHost="10.10.0.85";
                break;
            case "node86":
                dstHost="10.10.0.86";
                break;
            case "node87":
                dstHost="10.10.0.87";
                break;

            case "node90":
                dstHost="10.10.0.90";
                break;
            case "node91":
                dstHost="10.10.0.91";
                break;
            case "node92":
                dstHost="10.10.0.92";
                break;
            case "node93":
                dstHost="10.10.0.93";
                break;

            case "node95":
                dstHost="10.10.0.95";
                break;

            case "node97":
                dstHost="10.10.0.97";
                break;
            case "node98":
                dstHost="10.10.0.98";
                break;

            case "node105":
                dstHost="10.10.0.105";
                break;
            case "node106":
                dstHost="10.10.0.106";
                break;
            case "node108":
                dstHost="10.10.0.108";
                break;

            default:
                dstHost="";
                break;
        }
        return dstHost;
    }

    /**
     * ??????sendTimeLimit???????????????RDMAClient?????????FlushMessage
     */
    private void scheduleFlushMessage() {
        this.flushMessageTimerTask=new FlushMessageTimerTask();
        flushMessageTimer.schedule(flushMessageTimerTask,0,sendTimeLimit);
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
        timer.schedule(new TimerTask() {
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

    /**
     * Schedule a reconnect if we closed a non-null channel, and acquired the right to
     * provide a replacement by successfully setting a null to the channel field
     * @param channel
     * @return if the call scheduled a re-connect task
     */
    private boolean closeChannelAndReconnect(RdmaChannel channel) throws IOException, InterruptedException {
        if (channel != null) {
            channel.stop();
            if (channelRef.compareAndSet(channel, null)) {
                scheduleConnect(NO_DELAY_MS);
                return true;
            }
        }
        return false;
    }

    private boolean connectionEstablished(RdmaChannel channel) {
        return channel != null && channel.isConnected();
    }

    private void scheduleConnect(long delayMs) {
        //scheduler.newTimeout(new Connect(dstAddress), delayMs, TimeUnit.MILLISECONDS);
        connectThreadPool.submit(new Connect(dstAddress));
    }

    @Override
    public Status status() {
        if (closing) {
            return Status.Closed;
        } else if (!connectionEstablished(channelRef.get())) {
            return Status.Connecting;
        } else {
            if (channelRef.get().isConnected()) {
                return Status.Ready;
            } else {
                return Status.Connecting; // need to wait until sasl channel is also ready
            }
        }
    }

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

    ////////////////////////////////////??????transferAllGrouping/////////////////////////
    /**
     * Enqueue task messages to be sent to the remote destination (cf. `host` and `port`).
     */
    @Override
    public void send(WorkerMessage msgs) {
        LOG.debug("Client send msg : {}", msgs);
        if (closing) {
            int numMessages = msgs.tasks().size();
            LOG.error("discarding {} messages because the Netty client to {} is being closed", numMessages,
                    dstAddressPrefixedName);
            return;
        }

        if (!hasMessages(msgs)) {
            return;
        }

        RdmaChannel rdmaChannel = getConnectedChannel();
        if (rdmaChannel==null) {
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

            batcher.add(msgs);
            if (batcher.isFull()) {
                //If batcher Full FlushMessage
                if (rdmaChannel.isWritable()) {
                    MessageBatch drain = batcher.drain();
                    flushMessages(rdmaChannel, drain);

                    //??????flushMessageTimerTask????????????????????????????????????
                    flushMessageTimerTask.cancel();
                    scheduleFlushMessage();
                }else {
                    // Channel's buffer is full, meaning that we have time to wait other messages to arrive, and create a bigger
                    // batch. This yields better throughput.
                    // We0 can rely on `notifyInterestChanged` to push these messages as soon as there is spece in Netty's buffer
                    // because we know `Channel.isWritable` was false after the messages were already in the buffer.
                }
            }
        }

    }
    ////////////////////////////////////??????transferAllGrouping/////////////////////////////////

    private RdmaChannel getConnectedChannel() {
        RdmaChannel channel = channelRef.get();
        if (connectionEstablished(channel)) {
            return channel;
        } else {
            // Closing the channel and reconnecting should be done before handling the messages.
            boolean reconnectScheduled = false;
            try {
                reconnectScheduled = closeChannelAndReconnect(channel);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (reconnectScheduled) {
                // Log the connection error only once
                LOG.error("connection to {} is unavailable", dstAddressPrefixedName);
            }
            return null;
        }
    }

    private boolean hasMessages(WorkerMessage workerMessage) {
        return workerMessage != null && workerMessage.tasks().size()!=0 && workerMessage.message()!=null;
    }

    private void dropMessages(WorkerMessage msgs) {
        // We consume the iterator by traversing and thus "emptying" it.
        int msgCount = iteratorSize(msgs);
        messagesLost.getAndAdd(msgCount);
    }

    private int iteratorSize(WorkerMessage msgs) {
        return msgs.tasks().size();
    }

    private int flushMessagesTimes=0;

    /**
     * Asynchronously writes the message batch to the channel.
     *
     * If the write operation fails, then we will close the channel and trigger a reconnect.
     */
    private void flushMessages(RdmaChannel channel, MessageBatch full) {
        flushMessagesTimes++;
        try {
            ByteBuffer byteBuffer = full.buffer().toByteBuffer();
            RdmaBuffer rdmaBuffer = rdmaNode.getRdmaBufferManager().get(byteBuffer.capacity());
            rdmaBuffer.getByteBuffer().put(byteBuffer);

//            MessageDecoder messageDecoder=new MessageDecoder();
//            List<WorkerMessage> messages = (List<WorkerMessage>) messageDecoder.decode(rdmaBuffer.getByteBuffer());
            long startTime = System.nanoTime();
            channel.rdmaSendInQueue(new RdmaCompletionListener() {
                @Override
                public void onSuccess(ByteBuffer buf, Integer IMM) {
                    try {
                        long endTime = System.nanoTime();
                        bufferedWriter2.write((endTime-startTime)+"\n");
                        bufferedWriter2.flush();

                        rdmaBuffer.getByteBuffer().clear();
                        rdmaNode.getRdmaBufferManager().put(rdmaBuffer);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onFailure(Throwable exception) {
                    try {
                        rdmaBuffer.getByteBuffer().clear();
                        rdmaNode.getRdmaBufferManager().put(rdmaBuffer);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            },new long[]{rdmaBuffer.getAddress()},new int[]{rdmaBuffer.getLength()},new int[]{rdmaBuffer.getLkey()});
            LOG.debug("RDMAChannel flushMessages::done");

        } catch (Exception e) {
            e.printStackTrace();
        }

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
    public int getPort() {
        return dstAddress.getPort();
    }

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

    private void closeChannel() {
        RdmaChannel channel = channelRef.get();
        if (channel != null) {
            try {
                channel.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOG.debug("channel to {} closed", dstAddressPrefixedName);
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

    private String srcAddressName() {
        String name = null;
        return rdmaNode.getLocalInetSocketAddress().getHostName();
    }

    private String prefixedName(InetSocketAddress dstAddress) {
        if (null != dstAddress) {
            return PREFIX + dstAddress.toString();
        }
        return "";
    }

    private boolean reconnectingAllowed() {
        return !closing;
    }

    /**
     * Asynchronously establishes a RDMA connection to the remote address
     * This task runs on a single thread shared among all clients, and thus
     * should not perform operations that block.
     */
    private class Connect implements Runnable {

        private final InetSocketAddress address;

        public Connect(InetSocketAddress address) {
            this.address = address;
        }

        @Override
        public void run() {
            if (reconnectingAllowed()) {
                final int connectionAttempt = connectionAttempts.getAndIncrement();
                totalConnectionAttempts.getAndIncrement();

                LOG.debug("connecting to {} [attempt {}]", address.toString(), connectionAttempt);
                FutureTask futureTask=new FutureTask(new RDMAChannelConnectTask(address,connectionAttempt));
                futureTask.run();
            } else {
                close();
                throw new RuntimeException("Giving up to scheduleConnect to " + dstAddressPrefixedName + " after " +
                        connectionAttempts + " failed attempts. " + messagesLost.get() + " messages were lost");

            }
        }
    }

    private class RDMAChannelConnectTask implements Callable<RdmaChannel>{
        private final InetSocketAddress address;
        private final int connectionAttempt;

        public RDMAChannelConnectTask(InetSocketAddress address, int connectionAttempt) {
            this.address = address;
            this.connectionAttempt=connectionAttempt;
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
        public RdmaChannel call(){
            String hostName=address.getHostName();
            int port=address.getPort();
            RdmaChannel newChannel = null;
            try {
                newChannel = rdmaNode.getRdmaChannel(new InetSocketAddress(hostName, port), true, RdmaChannel.RdmaChannelType.RPC);
            } catch (Exception cause) {
                reschedule(cause);
                if (newChannel != null) {
                    try {
                        newChannel.stop();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } finally {
                if (connectionEstablished(newChannel)) {
                    boolean setChannel = channelRef.compareAndSet(null, newChannel);
                    checkState(setChannel);
                    LOG.debug("successfully connected to {}, {} [attempt {}]", address.toString(), newChannel.toString(),
                            connectionAttempt);
                    if (messagesLost.get() > 0) {
                        LOG.warn("Re-connection to {} was successful but {} messages has been lost so far", address.toString(), messagesLost.get());
                    }
                }
            }

            return newChannel;
        }
    }

    private static void checkState(boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

    /**
     * ????????????????????????MessageBatch??????Message
     * flushMessageTimerTask
     */
    private class FlushMessageTimerTask extends TimerTask{

        @Override
        public void run() {
            RdmaChannel rdmaChannel = getConnectedChannel();

            if (rdmaChannel == null) {
            /*
             * Connection is unavailable. We will drop pending messages and let at-least-once message replay kick in.
             *
             * Another option would be to buffer the messages in memory.  But this option has the risk of causing OOM errors,
             * especially for topologies that disable message acking because we don't know whether the connection recovery will
             * succeed  or not, and how long the recovery will take.
             */
                return;
            }

            synchronized (writeLock) {
                if (rdmaChannel.isWritable()) {
                    MessageBatch messageBatch = batcher.drain();
                    if (messageBatch != null) {
                        flushMessages(rdmaChannel, messageBatch);
                    }
                }else{
                    // Channel's buffer is full, meaning that we have time to wait other messages to arrive, and create a bigger
                    // batch. This yields better throughput.
                    // We0 can rely on `notifyInterestChanged` to push these messages as soon as there is spece in Netty's buffer
                    // because we know `Channel.isWritable` was false after the messages were already in the buffer.
                }
            }
        }
    }
}
