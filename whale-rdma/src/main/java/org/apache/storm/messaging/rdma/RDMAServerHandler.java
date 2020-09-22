package org.apache.storm.messaging.rdma;

import com.basic.rdmachannel.channel.RdmaChannel;
import com.basic.rdmachannel.channel.RdmaCompletionListener;
import com.basic.rdmachannel.channel.RdmaConnectListener;
import com.basic.rdmachannel.channel.RdmaNode;
import com.basic.rdmachannel.mr.RdmaBuffer;
import com.basic.rdmachannel.mr.RdmaBufferManager;
import org.apache.storm.Config;
import org.apache.storm.utils.ObjectReader;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * locate org.apache.storm.rdma
 * Created by MasterTj on 2019/4/10.
 */
public class RDMAServerHandler implements RdmaConnectListener {
    private IServer server;
    private MessageDecoder messageDecoder;
    private int messageBatchSize;
    private RdmaNode rdmaNode;
    private ExecutorService executorService;

    public RDMAServerHandler(IServer server, RdmaNode rdmaNode, Map<String, Object> topoConf) {
        this.server = server;
        this.messageDecoder=new MessageDecoder();
        this.rdmaNode=rdmaNode;
        this.messageBatchSize = ObjectReader.getInt(topoConf.get(Config.STORM_RDMA_MESSAGE_BATCH_SIZE), 262144);
        this.executorService= Executors.newCachedThreadPool();
    }


    @Override
    public void onSuccess(InetSocketAddress inetSocketAddress, RdmaChannel rdmaChannel) {
        executorService.submit(new RDMAServerHandlerTask(inetSocketAddress,rdmaChannel,rdmaNode.getRdmaBufferManager()));
    }

    @Override
    public void onFailure(Throwable exception) {
        exception.printStackTrace();
    }

    private class RDMAServerHandlerTask implements Runnable{
        private RdmaChannel rdmaChannel;
        private RdmaBufferManager rdmaBufferManager;
        private InetSocketAddress inetSocketAddress;

        public RDMAServerHandlerTask(InetSocketAddress inetSocketAddress, RdmaChannel rdmaChannel, RdmaBufferManager rdmaBufferManager) {
            this.inetSocketAddress=inetSocketAddress;
            this.rdmaChannel = rdmaChannel;
            this.rdmaBufferManager=rdmaBufferManager;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    RdmaBuffer rdmaBuffer = rdmaBufferManager.get(Math.min(10*1024*1024,messageBatchSize*2));
                    rdmaChannel.rdmaReceiveInQueue(new RdmaCompletionListener() {
                        @Override
                        public void onSuccess(ByteBuffer buf, Integer IMM) {
                            try {
                                Object decode = messageDecoder.decode(rdmaBuffer.getByteBuffer());
                                server.received(decode, inetSocketAddress.getHostName(), rdmaChannel);
                                rdmaBufferManager.put(rdmaBuffer);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        @Override
                        public void onFailure(Throwable exception) {
                            try {
                                exception.printStackTrace();
                                rdmaBufferManager.put(rdmaBuffer);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }, rdmaBuffer.getAddress(), rdmaBuffer.getLength(), rdmaBuffer.getLkey());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
