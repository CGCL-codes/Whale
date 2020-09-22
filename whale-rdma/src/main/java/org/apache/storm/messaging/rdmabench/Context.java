package org.apache.storm.messaging.rdmabench;

import org.apache.commons.io.FileUtils;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IContext;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * locate org.apache.storm.messaging.rdmabench
 * Created by mastertj on 2018/9/11.
 */
public class Context implements IContext {
    private Map<String, Object> topoConf;
    private Map<String, IConnection> connections;

    @Override
    public void prepare(Map<String, Object> topoConf) {
        this.topoConf=topoConf;
        connections=new HashMap<>();
        try {
            File rdmabenchFile=new File("/whale/rdmabench");
            FileUtils.deleteDirectory(rdmabenchFile);
            rdmabenchFile.mkdir();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * establish a server with a binding port
     */
    @Override
    public synchronized IConnection bind(String storm_id, int port) {
        IConnection server = null;
        try {
            server = new Server(topoConf, port);
            connections.put(key(storm_id, server.getPort()), server);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return server;
    }

    /**
     * establish a connection to a remote server
     */
    @Override
    public synchronized IConnection connect(String storm_id, String host, int port){
        IConnection client=null;
        try {
            IConnection connection = connections.get(key(host,port));
            if(connection !=null)
            {
                return connection;
            }
            client = new Client(topoConf, host, port, this);
            connections.put(key(host, client.getPort()), client);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    synchronized void removeClient(String host, int port) {
        if (connections != null) {
            connections.remove(key(host, port));
        }
    }

    /**
     * terminate this context
     */
    @Override
    public synchronized void term() {
        for (IConnection conn : connections.values()) {
            conn.close();
        }

        connections = null;
    }

    private String key(String host, int port) {
        return String.format("%s:%d", host, port);
    }
}
