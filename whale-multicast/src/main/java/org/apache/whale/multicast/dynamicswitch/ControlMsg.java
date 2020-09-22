package org.apache.whale.multicast.dynamicswitch;

import com.google.gson.Gson;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import java.util.HashMap;

/**
 * locate org.apache.storm.multicast.dynamicswitch
 * Created by master on 2019/10/31.
 * 控制信息,包括如下信息
 * 1。动态切换之后的多播结构图
 * 2。动态切换状态信息
 * 3。用于动态切换的控制信息
 */
public class ControlMsg {
    private DirectedAcyclicGraph<String, DefaultEdge> graph;
    private String currentStatus;
    private HashMap<String, ControlConfiguration> controlConfiguration;

    public ControlMsg(DirectedAcyclicGraph<String, DefaultEdge> graph, String currentStatus, HashMap<String, ControlConfiguration> controlConfiguration) {
        this.graph = graph;
        this.currentStatus = currentStatus;
        this.controlConfiguration = controlConfiguration;
    }

    public DirectedAcyclicGraph<String, DefaultEdge> getGraph() {
        return graph;
    }

    public HashMap<String, ControlConfiguration> getControlConfiguration() {
        return controlConfiguration;
    }

    public String getCurrentStatus() {
        return currentStatus;
    }

    public static String objectToJson(ControlMsg controlMsg){
        Gson gson = new Gson();
        return gson.toJson(controlMsg);
    }

    public static ControlMsg jsonToObject(String json){
        Gson gson = new Gson();
        return gson.fromJson(json, ControlMsg.class);
    }
}

