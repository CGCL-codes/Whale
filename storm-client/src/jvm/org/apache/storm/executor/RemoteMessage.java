package org.apache.storm.executor;

import org.apache.storm.generated.NodeInfo;
import org.apache.storm.messaging.WorkerMessage;

/**
 * locate org.apache.storm.executor
 * Created by tjmaster on 18-2-24.
 */
public class RemoteMessage {
    private NodeInfo nodeInfo;
    private WorkerMessage workerMessage;

    public RemoteMessage() {
    }

    public RemoteMessage(NodeInfo nodeInfo, WorkerMessage workerMessage) {
        this.nodeInfo = nodeInfo;
        this.workerMessage = workerMessage;
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    public void setNodeInfo(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public WorkerMessage getWorkerMessage() {
        return workerMessage;
    }

    public void setWorkerMessage(WorkerMessage workerMessage) {
        this.workerMessage = workerMessage;
    }

    @Override
    public String toString() {
        return "RemoteMessage{" +
                "nodeInfo=" + nodeInfo +
                ", workerMessage=" + workerMessage +
                '}';
    }
}
