package org.apache.storm.multicast.core;

import org.apache.storm.multicast.dynamicswitch.ControlMsg;
import org.apache.storm.multicast.dynamicswitch.DynamicSwitchController;
import org.apache.storm.multicast.util.ModelConstants;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

/**
 * locate org.apache.storm.multicast.core
 * Created by master on 2020/5/25.
 * 多播控制器，主要有两个用途
 * 1。根据当前流输入速率，确定系统最大出度
 * 2。根据传输队列长度变化趋势，判断是否进行流多播动态切换
 */
public class MulticastController {
    /**
     * 计算当前流输入速率下，系统最大出度
     * @param inputRate
     * @param queueCapacity
     * @return
     */
    public static int computeMaxOutDegree(double inputRate, int queueCapacity){
        double critical = 0.0;
        queueCapacity = queueCapacity > 0 ? queueCapacity : ModelConstants.queueCapacity;
        critical = 2*queueCapacity / (ModelConstants.tupleProcessTime * (inputRate*queueCapacity + inputRate - Math.sqrt(inputRate*inputRate*queueCapacity*queueCapacity + inputRate*inputRate)));
        System.out.println(critical);
        return (int)critical;
    }

    /**
     * 决定是否要进行流多播结构动态调整机制
     * @param previousLength
     * @param currentLength
     * @return
     */
    public static String determineSelfAdjustment(long previousLength, long currentLength){
        if (previousLength < currentLength) {
            double deltaLength = currentLength - previousLength;
            if( deltaLength / (double)(ModelConstants.warningWaterLine-currentLength) > ModelConstants.threshold_down){
                return ModelConstants.NegativeSacleDown;
            }else return null;
        } else if (previousLength > currentLength) {
            double deltaLength = currentLength - previousLength;
            if( deltaLength / (double)(previousLength) > ModelConstants.threshold_up){
                return ModelConstants.ActiveSacleUp;
            }else return null;
        } else {
            return null;
        }
    }

    /**
     * 根据先前的多播结构的最大出度和新的最大出度得到相应的控制信息
     * @param graph
     * @param oldDegree
     * @param newDegree
     * @return
     */
    public static ControlMsg generateControlMsg(DirectedAcyclicGraph<String, DefaultEdge> graph, int oldDegree, int newDegree) {
        return DynamicSwitchController.change(graph,oldDegree,newDegree);
    }

}
