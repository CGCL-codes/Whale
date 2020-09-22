package org.apache.storm.multicast.core;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.multicast.model.BalancedPartialMulticastGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * locate org.apache.storm.multicast.core
 * Created by MasterTj on 2019/10/9.
 */
public class SchedulingTopologyBuilder extends TopologyBuilder {
    private static Logger logger= LoggerFactory.getLogger(SchedulingTopologyBuilder.class);

    public static final String BROADCAST_BOLT="broadcastbolt";

    /**
     * 构造Broadcast二项式树模型
     * @param destinationNum    下游广播Bolt的数量
     * @param workersNum         Topology中Worker的数量
     * @param UPStreamCompoentID    上游StreamComponentID
     * @param UpStreamID        上游数据流StreamID
     * @param BroadcastStreamID BroadCast-Bolt数据流StreamID
     * @param tClass    BroadCast-Bolt的Class对象
     * @param <T>   BroadCast-Bolt模板
     */
    public <T extends BaseRichBolt> BalancedPartialMulticastGraph<String> constructionBinomialTree(int destinationNum, int workersNum, String UPStreamCompoentID, String UpStreamID, String BroadcastStreamID, Class<T> tClass){
        BalancedPartialMulticastGraph<String> dag = new BalancedPartialMulticastGraph<String>(DefaultEdge.class, Integer.MAX_VALUE);
        try {
            int id=0,layer=0,parallelism=0,divisorNum=destinationNum/workersNum,modNum= destinationNum%workersNum;
            List<String> list=new ArrayList<>();
            list.add(UPStreamCompoentID);
            dag.addVertex(UPStreamCompoentID);
            dag.getLayerElementMap().put(layer, Arrays.asList(UPStreamCompoentID));
            logger.info("sourceID:{}, layer:{}, parallelism:{}",UPStreamCompoentID,layer,1);

            while (id <= workersNum){
                int size = list.size();
                layer++;
                List<String> elementList=new ArrayList<>();
                for (int index = 0; index < size; index++) {
                    String upsource = list.get(index);
                    T bolt = tClass.newInstance();
                    id++;
                    parallelism = (modNum-- > 0) ? divisorNum+1 : divisorNum;
                    String broadcast_bolt_ID = BROADCAST_BOLT+"-"+id+"-"+layer+"-"+(elementList.size()+1);
                    logger.info("broadcastID:{}, layer:{}, parallelism:{}",broadcast_bolt_ID,layer,parallelism);

                    if(upsource.equals(UPStreamCompoentID))
                        this.setBolt(broadcast_bolt_ID,bolt,parallelism).allGrouping(UPStreamCompoentID,UpStreamID);
                    else
                        this.setBolt(broadcast_bolt_ID,bolt,parallelism).allGrouping(upsource,BroadcastStreamID);
                    list.add(broadcast_bolt_ID);
                    elementList.add(broadcast_bolt_ID);

                    dag.addVertex(broadcast_bolt_ID);
                    dag.addEdge(upsource,broadcast_bolt_ID);
                    //return
                    if(id == workersNum) {
                        dag.getLayerElementMap().put(layer, elementList);
                        return dag;
                    }
                }
                dag.getLayerElementMap().put(layer, elementList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dag;
    }

    /**
     * 构造BalancedPartialMulticast模型
     * @param destinationNum 下游广播Bolt的数量
     * @param degree 树节点的出度数量
     * @param UPStreamCompoentID  上游StreamComponentID
     * @param UpStreamID 上游数据流StreamID
     * @param BroadcastStreamID BroadCast-Bolt数据流StreamID
     * @param tClass BroadCast-Bolt的Class对象
     * @param <T> BroadCast-Bolt模板BroadCast-Bolt模板
     */
    public <T extends BaseRichBolt> BalancedPartialMulticastGraph<String> constructionBalancedPartialMulticast(int destinationNum, int workersNum, int degree, String UPStreamCompoentID, String UpStreamID, String BroadcastStreamID, Class<T> tClass) {
        BalancedPartialMulticastGraph<String> dag = new BalancedPartialMulticastGraph<String>(DefaultEdge.class, degree);
        try {
            int id=0,layer=0,parallelism=0,divisorNum=destinationNum/workersNum,modNum= destinationNum%workersNum;
            List<String> list=new ArrayList<>();
            list.add(UPStreamCompoentID);
            dag.addVertex(UPStreamCompoentID);
            dag.getLayerElementMap().put(layer, Arrays.asList(UPStreamCompoentID));
            logger.info("sourceID:{}, layer:{}, parallelism:{}",UPStreamCompoentID,layer,1);

            while (id <= workersNum){
                int size = list.size();
                layer++;
                List<String> elementList=new ArrayList<>();
                for (int index = 0; index < size; index++) {
                    String upsource = list.get(index);
                    if(dag.outDegreeOf(upsource) < degree){
                        T bolt = tClass.newInstance();
                        id++;
                        parallelism = (modNum-- > 0) ? divisorNum+1 : divisorNum;
                        String broadcast_bolt_ID = BROADCAST_BOLT+"-"+id+"-"+layer+"-"+(elementList.size()+1);
                        logger.info("broadcastID:{}, layer{}, parallelism:{}",broadcast_bolt_ID,layer,parallelism);

                        if (upsource.equals(UPStreamCompoentID))
                            this.setBolt(broadcast_bolt_ID, bolt, parallelism).allGrouping(UPStreamCompoentID, UpStreamID);
                        else
                            this.setBolt(broadcast_bolt_ID, bolt, parallelism).allGrouping(upsource, BroadcastStreamID);
                        list.add(broadcast_bolt_ID);
                        elementList.add(broadcast_bolt_ID);

                        dag.addVertex(broadcast_bolt_ID);
                        dag.addEdge(upsource,broadcast_bolt_ID);
                    }
                    //return
                    if(id == workersNum) {
                        dag.getLayerElementMap().put(layer, elementList);
                        return dag;
                    }
                }
                dag.getLayerElementMap().put(layer, elementList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dag;
    }

    /**
     * 构造Chain 链式模型
     * @param destinationNum 下游广播Bolt的数量
     * @param workersNum         Topology中Worker的数量
     * @param UPStreamCompoentID 上游StreamComponentID
     * @param UpStreamID 上游数据流StreamID
     * @param BroadcastStreamID BroadCast-Bolt数据流StreamID
     * @param tClass  BroadCast-Bolt的Class对象
     * @param <T> BroadCast-Bolt模板BroadCast-Bolt模板
     */
    public <T extends BaseRichBolt> BalancedPartialMulticastGraph<String> constructionChain(int destinationNum, int workersNum, String UPStreamCompoentID, String UpStreamID, String BroadcastStreamID, Class<T> tClass) {
        BalancedPartialMulticastGraph<String> dag = new BalancedPartialMulticastGraph<String>(DefaultEdge.class, 1);
        try {
            int parallelism=0,layer=0,divisorNum=destinationNum/workersNum,modNum= destinationNum%workersNum;
            dag.addVertex(UPStreamCompoentID);
            dag.getLayerElementMap().put(layer, Arrays.asList(UPStreamCompoentID));
            logger.info("sourceID:{}, layer:{}, parallelism:{}",UPStreamCompoentID,layer,1);

            for (int id = 1; id <= workersNum; id++) {
                layer++;
                T bolt = tClass.newInstance();
                parallelism = (modNum-- > 0) ? divisorNum+1 : divisorNum;
                String upstream_component_ID = (id==1)? UPStreamCompoentID:BROADCAST_BOLT+"-"+(id-1)+"-"+(layer-1)+"-"+1;
                String broadcast_bolt_ID = BROADCAST_BOLT+"-"+id+"-"+layer+"-"+1;
                logger.info("broadcastID:{}, layer:{}, parallelism:{}",broadcast_bolt_ID,layer,parallelism);

                dag.addVertex(broadcast_bolt_ID);
                if(id == 1) {
                    this.setBolt(broadcast_bolt_ID, bolt, parallelism).allGrouping(upstream_component_ID, UpStreamID);
                    dag.addEdge(upstream_component_ID,broadcast_bolt_ID);
                }
                else {
                    this.setBolt(broadcast_bolt_ID, bolt, parallelism).allGrouping(upstream_component_ID, BroadcastStreamID);
                    dag.addEdge(upstream_component_ID,broadcast_bolt_ID);
                }
                dag.getLayerElementMap().put(layer, Arrays.asList(broadcast_bolt_ID));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dag;
    }

    /**
     * 构造Broadcast 顺序模型
     * @param destinationNum 下游广播Bolt的数量
     * @param workersNum         Topology中Worker的数量
     * @param UPStreamCompoentID 上游StreamComponentID
     * @param UpStreamID 上游数据流StreamID
     * @param tClass BroadCast-Bolt的Class对象
     * @param <T> BroadCast-Bolt模板
     */
    public <T extends BaseRichBolt> BalancedPartialMulticastGraph<String> constructionSequential(int destinationNum, int workersNum, String UPStreamCompoentID, String UpStreamID, Class<T> tClass){
        // -1 表示特殊的平衡偏序广播模型（顺序广播模型）
        BalancedPartialMulticastGraph<String> dag = new BalancedPartialMulticastGraph<String>(DefaultEdge.class, -1);
        try {
            int parallelism=0,layer=0,divisorNum=destinationNum/workersNum,modNum= destinationNum%workersNum;
            dag.addVertex(UPStreamCompoentID);
            dag.getLayerElementMap().put(layer++, Arrays.asList(UPStreamCompoentID));
            logger.info("sourceID:{}, layer:{}, parallelism:{}",UPStreamCompoentID,layer,1);

            List<String> elementList=new ArrayList<>();
            for (int id= 1; id <= workersNum; id++) {
                T bolt = tClass.newInstance();
                parallelism = (modNum-- > 0) ? divisorNum+1 : divisorNum;

                String broadcast_bolt_ID = BROADCAST_BOLT+"-"+id+"-"+layer+"-"+id;
                logger.info("broadcastID:{}, layer:{}, parallelism:{}",broadcast_bolt_ID,layer,parallelism);

                this.setBolt(broadcast_bolt_ID, bolt, parallelism).allGrouping(UPStreamCompoentID,UpStreamID);
                elementList.add(broadcast_bolt_ID);

                dag.addVertex(broadcast_bolt_ID);
                dag.addEdge(UPStreamCompoentID,broadcast_bolt_ID);
            }
            dag.getLayerElementMap().put(layer, elementList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dag;
    }

    /**
     * 构造Broadcast K-二项式树模型
     * @param KIndex k-二项式源头index
     * @param workersNum         Topology中Worker的数量
     * @param destinationNum    下游广播Bolt的数量
     * @param workersNum         Topology中Worker的数量
     * @param UPStreamCompoentID    上游StreamComponentID
     * @param UpStreamID        上游数据流StreamID
     * @param BroadcastStreamID BroadCast-Bolt数据流StreamID
     * @param tClass    BroadCast-Bolt的Class对象
     * @param <T>   BroadCast-Bolt模板
     */
    public <T extends BaseRichBolt> void constructionKBinomialTree(int KIndex, int destinationNum, int workersNum, String UPStreamCompoentID, String UpStreamID, String BroadcastStreamID, Class<T> tClass){
        try {
            List<String> list=new ArrayList<>();
            list.add(UPStreamCompoentID);
            int id=0,layer=0,parallelism=0,divisorNum=destinationNum/workersNum,modNum= destinationNum%workersNum;
            logger.info("sourceID:{}, layer:{}, parallelism:{}",UPStreamCompoentID,layer,1);

            while (id <= workersNum){
                int size = list.size();
                layer++;
                for (int index = 0; index < size; index++) {
                    String upsource = list.get(index);
                    T bolt = tClass.newInstance();
                    id++;
                    parallelism = (modNum-- > 0) ? divisorNum+1 : divisorNum;
                    String broadcast_bolt_ID = KIndex+"-"+BROADCAST_BOLT+"-"+id+"-"+layer+"-"+(index+1);
                    logger.info("broadcatID:{}, parallelism:{}",broadcast_bolt_ID,parallelism);

                    if(upsource.equals(UPStreamCompoentID))
                        this.setBolt(broadcast_bolt_ID,bolt,parallelism).allGrouping(UPStreamCompoentID,UpStreamID);
                    else
                        this.setBolt(broadcast_bolt_ID,bolt,parallelism).allGrouping(upsource,BroadcastStreamID);
                    list.add(BROADCAST_BOLT+"-"+id+"-"+layer+"-"+(index+1));

                    //return
                    if(id == workersNum)
                        return;
                }
            }
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
