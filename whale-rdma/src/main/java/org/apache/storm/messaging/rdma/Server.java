package org.apache.storm.messaging.rdma;

import com.basic.rdmachannel.RDMANodeContainer;
import com.basic.rdmachannel.channel.RdmaChannel;
import com.basic.rdmachannel.channel.RdmaNode;
import org.apache.storm.grouping.Load;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.IConnectionCallback;
import org.apache.storm.messaging.TaskMessage;
import org.apache.storm.messaging.WorkerMessage;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * locate org.apache.storm.rdma
 * Created by mastertj on 2018/9/11.
 */
public class Server extends ConnectionWithStatus implements IStatefulObject, IServer {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    @SuppressWarnings("rawtypes")
    Map<String, Object> topoConf;
    int port;
    private final ConcurrentHashMap<String, AtomicInteger> messagesEnqueued = new ConcurrentHashMap<>();
    private final AtomicInteger messagesDequeued = new AtomicInteger(0);

    private volatile boolean closing = false;
    List<TaskMessage> closeMessage = Arrays.asList(new TaskMessage(-1, null));
    private KryoValuesSerializer _ser;
    private IConnectionCallback _cb = null;
    //private final int boundPort;

    //RDMANode and RDMA Channel
    private RdmaNode rdmaNode;

    public Server(Map<String, Object> topoConf, int port) throws Exception {
        this.topoConf = topoConf;
        this.port = port;
        RDMANodeContainer.setPort(port);
        this.rdmaNode= RDMANodeContainer.getInstance();

        //TODO
        rdmaNode.bindConnectCompleteListener(new RDMAServerHandler(this,rdmaNode,topoConf));
        // Bind and start to accept incoming connections.
    }

    @Override
    public Status status() {
        if (closing) {
            return Status.Closed;
        } else if (!connectionEstablished(rdmaNode.passiveRdmaChannelMap.values().iterator())) {
            return Status.Connecting;
        } else {
            return Status.Ready;
        }
    }

    private boolean connectionEstablished(RdmaChannel channel) {
        return channel != null && channel.isConnected();
    }


    private boolean connectionEstablished(Iterator<RdmaChannel> iterator) {
        boolean allEstablished = true;
       while (iterator.hasNext()){
            if (!(connectionEstablished(iterator.next()))) {
                allEstablished = false;
                break;
            }
        }
        return allEstablished;
    }
    @Override
    public void registerRecv(IConnectionCallback cb) {
        _cb = cb;
    }

    @Override
    public void sendLoadMetrics(Map<Integer, Double> taskToLoad) {
//        try {
//            MessageBatch mb = new MessageBatch(1);
//            mb.add(new WorkerMessage(Arrays.asList(-1), _ser.serialize(Arrays.asList((Object)taskToLoad))));
//            allChannels.write(mb);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    @Override
    public void send(int taskId, byte[] payload) {
        throw new UnsupportedOperationException("Server connection should not send any messages");
    }

    @Override
    public void send(WorkerMessage msgs) {
        throw new UnsupportedOperationException("Server connection should not send any messages");
    }

    @Override
    public Map<Integer, Load> getLoad(Collection<Integer> tasks) {
        return null;
    }

    @Override
    public int getPort() {
        return port;
    }


    @Override
    public void close() {
        Iterator<RdmaChannel> iterator = rdmaNode.passiveRdmaChannelMap.values().iterator();
        while (iterator.hasNext()){
            RdmaChannel rdmaChannel = iterator.next();
            try {
                rdmaChannel.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
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

    public void received(Object message, String remote, RdmaChannel channel)  throws InterruptedException {
        List<WorkerMessage> messages = (List<WorkerMessage>) message;
        enqueue(messages, remote);
    }

    @Override
    public void closeChannel(RdmaChannel c) {
        try {
            c.stop();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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
}
