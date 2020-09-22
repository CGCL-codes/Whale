package org.apache.storm.multicast.model;

import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.storm.multicast.io.*;
import org.jgrapht.Graph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * locate org.apache.storm.multicast.model
 * Created by MasterTj on 2019/10/11.
 * 平衡偏序广播模型
 */
public class BalancedPartialMulticastGraph<V> extends DirectedAcyclicGraph<V,DefaultEdge> {

    //广播模型最大出度
    private int maxDegree;

    private Map<Integer,List<V>> layerElementMap = new HashMap<>();

    public BalancedPartialMulticastGraph(Class arg0, int maxDegree) {
        super(arg0);
        this.maxDegree = maxDegree;
    }

    /**
     * 向平衡偏序广播模型添加一个元素
     * @param bolt
     * @return
     */
    public boolean addElement(V bolt) {
        return super.addVertex(bolt);
    }

    /**
     * 获取位于当前层数layer的所有元素
     * Source的 layer为0
     * @param layer
     * @return
     */
    public List<V> getElementByLayer(int layer){
        if(layer > getLayer()){
            return null;
        }
        return layerElementMap.get(layer);
    }

    public int getMaxDegree() {
        return maxDegree;
    }

    public void setMaxDegree(int maxDegree) {
        this.maxDegree = maxDegree;
    }

    /**
     * 获取当前层数 最大层数（出去Source Layer0）
     * @return
     */
    public int getLayer() {
        return layerElementMap.size()-1;
    }

    public Map<Integer, List<V>> getLayerElementMap() {
        return layerElementMap;
    }

    public void setLayerElementMap(Map<Integer, List<V>> layerElementMap) {
        this.layerElementMap = layerElementMap;
    }

    @Override
    public String toString() {
        return "BalancedPartialMulticastGraph{" +
                "maxDegree=" + maxDegree +
                ", layerElementMap=" + layerElementMap +
                '}';
    }

    /**
     * multicastGraph 转换成 Json字符串
     * @param multicastGraph 广播模型
     * @param <V> 顶点模板
     * @return
     * @throws ExportException
     */
    public static <V> String graphToJson(BalancedPartialMulticastGraph<V> multicastGraph) throws ExportException {
        Gson gson=new Gson();

        ComponentNameProvider<V> vertexIdProvider = new ComponentNameProvider<V>() {

            @Override
            public String getName(V component) {
                return component.toString();
            }
        };

        GraphExporter<V, DefaultEdge> exporter =
                new JSONExporter<V, DefaultEdge>(vertexIdProvider);
        Writer writer = new StringWriter();
        exporter.exportGraph(multicastGraph, writer);
        IOUtils.closeQuietly(writer);
        String json = writer.toString();

        GsonObject<V> gsonObject=new GsonObject<V>(json,multicastGraph.maxDegree,multicastGraph.getLayerElementMap());
        return gson.toJson(gsonObject);
    }

    /**
     * json转换为MulticatGraph
     * @param json json字符串
     * @param <V> 顶点V模板
     * @return
     * @throws ExportException
     * @throws ImportException
     */
    public static <V> BalancedPartialMulticastGraph<V> jsonToGraph(String json) throws ExportException, ImportException {
        Gson gson=new Gson();
        GsonObject<V> gsonObject = gson.fromJson(json, GsonObject.class);
        BalancedPartialMulticastGraph<V> multicastGraph;
        VertexProvider<V> vertexProvider = new VertexProvider<V>() {

            @Override
            public V buildVertex(String id, Map<String, Attribute> attributes) {
                return (V) id;
            }
        };

        EdgeProvider<V, DefaultEdge> edgeProvider = new EdgeProvider<V, DefaultEdge>() {

            @Override
            public DefaultEdge buildEdge(V from, V to, String label,
                                         Map<String, Attribute> attributes) {
                Graph<V, DefaultEdge> tmpGraph =
                        new DirectedAcyclicGraph<V, DefaultEdge>(DefaultEdge.class);
                tmpGraph.addVertex(from);
                tmpGraph.addVertex(to);
                tmpGraph.addEdge(from, to);
                return tmpGraph.edgeSet().iterator().next();
            }
        };

        GraphImporter<V, DefaultEdge> importer =
                new JSONImporter<V, DefaultEdge>(vertexProvider, edgeProvider);

        multicastGraph = new BalancedPartialMulticastGraph<>(DefaultEdge.class,gsonObject.maxDegree);
        Reader reader = new StringReader(gsonObject.getDagJson());
        importer.importGraph(multicastGraph, reader);
        IOUtils.closeQuietly(reader);
        multicastGraph.setLayerElementMap(gsonObject.layerElementMap);
        return multicastGraph;
    }

    public static class GsonObject<V> implements Serializable {
        private String dagJson;
        private int maxDegree;
        private Map<Integer,List<V>> layerElementMap;

        public GsonObject(String dagJson) {
            this.dagJson = dagJson;
        }

        public GsonObject(String dagJson, int maxDegree, Map<Integer, List<V>> layerElementMap) {
            this.dagJson = dagJson;
            this.maxDegree = maxDegree;
            this.layerElementMap = layerElementMap;
        }

        public String getDagJson() {
            return dagJson;
        }

        public void setDagJson(String dagJson) {
            this.dagJson = dagJson;
        }

        public int getMaxDegree() {
            return maxDegree;
        }

        public void setMaxDegree(int maxDegree) {
            this.maxDegree = maxDegree;
        }

        public Map<Integer, List<V>> getLayerElementMap() {
            return layerElementMap;
        }

        public void setLayerElementMap(Map<Integer, List<V>> layerElementMap) {
            this.layerElementMap = layerElementMap;
        }

        @Override
        public String toString() {
            return "GsonObject{" +
                    "dagJson='" + dagJson + '\'' +
                    ", maxDegree=" + maxDegree +
                    ", layerElementMap=" + layerElementMap +
                    '}';
        }
    }

}
