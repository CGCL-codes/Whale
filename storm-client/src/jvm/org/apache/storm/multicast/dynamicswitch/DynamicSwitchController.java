package org.apache.storm.multicast.dynamicswitch;

import org.apache.storm.multicast.model.BalancedPartialMulticastGraph;
import org.apache.storm.multicast.util.ModelConstants;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import java.util.*;

public class DynamicSwitchController {
    public static ControlMsg change(DirectedAcyclicGraph<String, DefaultEdge> graph, int oldDegree, int newDegree) {
        if (oldDegree > newDegree) {
            return scaleDown(graph, newDegree);
        } else if (oldDegree < newDegree) {
            return scaleUp(graph, newDegree);
        } else {
            return new ControlMsg(graph, null, new HashMap<>());
        }
    }

    private static ControlMsg scaleDown(DirectedAcyclicGraph<String, DefaultEdge> graph, int newDegree) {
        Set<String> oldVertexSet = graph.vertexSet();
        String root = oldVertexSet.iterator().next();
        if (graph.outDegreeOf(root) <= newDegree)
            return new ControlMsg(graph, ModelConstants.NegativeSacleDown, new HashMap<>());

        HashMap<String, ControlConfiguration> changes = new HashMap<>();
        BalancedPartialMulticastGraph<String> newGraph = new BalancedPartialMulticastGraph<>(DefaultEdge.class, newDegree);
        Deque<String> disconnectQueue = new LinkedList<>();

        // 断开大于newDegree的分支
        Queue<String> dfiQueue = new LinkedList<>();
        dfiQueue.offer(root);
        while (!dfiQueue.isEmpty()) {
            String v = dfiQueue.poll();
            String[] vSplit = v.split("-");
            Iterator<DefaultEdge> iter = graph.outgoingEdgesOf(v).iterator();
            for (int i = 0; i < newDegree; i++) {
                if (!iter.hasNext())
                    break;
                dfiQueue.offer(graph.getEdgeTarget(iter.next()));
            }
            ArrayList<DefaultEdge> removeEdges = new ArrayList<>();
            while (iter.hasNext()) {
                DefaultEdge t = iter.next();
                String rm = graph.getEdgeTarget(t);
                String[] rmSplit = rm.split("-");
                disconnectQueue.offerLast(rm);
                changes.put(rmSplit[0], new ControlConfiguration().setDisconnect(vSplit[0]));
                removeEdges.add(t);
            }
            removeEdges.forEach(graph::removeEdge);
        }

        // 新图
        ArrayList<String> nodeArray = new ArrayList<>(oldVertexSet.size());
        HashMap<String, Iterator<DefaultEdge>> oldVertexEdgesIter = new HashMap<>();
        newGraph.addVertex(root);
        nodeArray.add(root);
        oldVertexEdgesIter.put(root, graph.outgoingEdgesOf(root).iterator());
        for (int id = 2, layer = 1; id <= oldVertexSet.size(); layer++) {
            ArrayList<String> temp = new ArrayList<>(nodeArray.size());
            Iterator<String> iter = nodeArray.iterator();
            int layerIndex = 1;
            while (iter.hasNext()) {
                if (id > oldVertexSet.size())
                    break;

                String v = iter.next();

                if (newGraph.outDegreeOf(v) == newDegree) {
                    iter.remove();
                    continue;
                }

                String newV;
                Iterator<DefaultEdge> oldEdges = oldVertexEdgesIter.get(v);
                if (oldEdges != null && oldEdges.hasNext()) {
                    String oldV = graph.getEdgeTarget(oldEdges.next());
                    String[] split = oldV.split("-");
                    if (Integer.parseInt(split[1]) != id || Integer.parseInt(split[2]) != layer || Integer.parseInt(split[3]) != layerIndex) {
                        if (changes.containsKey(split[0])) {
                            changes.get(split[0]).setMeta(id, layer, layerIndex);
                        } else {
                            changes.put(split[0], new ControlConfiguration().setMeta(id, layer, layerIndex));
                        }
                    }
                    newV = split[0] + "-" + id + "-" + layer + "-" + layerIndex;
                    oldVertexEdgesIter.put(newV, graph.outgoingEdgesOf(oldV).iterator());
                } else {
                    String oldV = disconnectQueue.pollFirst();
                    String[] oSplit = oldV.split("-");
                    graph.outgoingEdgesOf(oldV).forEach(e -> {
                        String next = graph.getEdgeTarget(e);
                        String[] split = next.split("-");
                        changes.put(split[0], new ControlConfiguration().setDisconnect(oSplit[0]));
                        disconnectQueue.offerFirst(next);
                    });
                    changes.get(oSplit[0]).setReconnect(v.split("-")[0]).setMeta(id, layer, layerIndex);
                    newV = oSplit[0] + "-" + id + "-" + layer + "-" + layerIndex;
                }
                temp.add(newV);
                newGraph.addVertex(newV);
                newGraph.addEdge(v, newV);
                id++;
                layerIndex++;
            }
            nodeArray.addAll(temp);
        }
        return new ControlMsg(newGraph, ModelConstants.NegativeSacleDown, changes);
    }

    private static ControlMsg scaleUp(DirectedAcyclicGraph<String, DefaultEdge> graph, int newDegree) {
        HashMap<String, ControlConfiguration> changes = new HashMap<>();
        BalancedPartialMulticastGraph<String> newGraph = new BalancedPartialMulticastGraph<>(DefaultEdge.class,newDegree);

        ArrayList<String[]> oldVertexArray = new ArrayList<>(graph.vertexSet().size());
        graph.vertexSet().stream()
                .map(v -> v.split("-"))
                .sorted((a, b) -> Integer.parseInt(b[1]) - Integer.parseInt(a[1]))
                .forEach(oldVertexArray::add);

        String root = String.join("-", oldVertexArray.get(oldVertexArray.size() - 1));

        ArrayList<String> nodeArray = new ArrayList<>(oldVertexArray.size());
        HashMap<String, Iterator<DefaultEdge>> oldVertexEdgesIter = new HashMap<>();
        Iterator<String[]> oldVertexIter = oldVertexArray.iterator();
        nodeArray.add(root);
        newGraph.addVertex(root);
        oldVertexEdgesIter.put(root, graph.outgoingEdgesOf(root).iterator());

        for (int id = 2, layer = 1; id <= oldVertexArray.size(); layer++) {
            ArrayList<String> temp = new ArrayList<>(nodeArray.size());
            Iterator<String> iter = nodeArray.iterator();
            int layerIndex = 1;
            while (iter.hasNext()) {
                if (id > oldVertexArray.size())
                    break;

                String v = iter.next();

                if (newGraph.outDegreeOf(v) == newDegree) {
                    iter.remove();
                    continue;
                }

                String newV;
                Iterator<DefaultEdge> oldEdges = oldVertexEdgesIter.get(v);
                if (oldEdges != null && oldEdges.hasNext()) {
                    String ov = graph.getEdgeTarget(oldEdges.next());
                    String[] split = ov.split("-");
                    if (Integer.parseInt(split[1]) != id || Integer.parseInt(split[2]) != layer || Integer.parseInt(split[3]) != layerIndex) {
                        if (changes.containsKey(split[0])) {
                            changes.get(split[0]).setMeta(id, layer, layerIndex);
                        } else {
                            changes.put(split[0], new ControlConfiguration().setMeta(id, layer, layerIndex));
                        }
                    }
                    newV = split[0] + "-" + id + "-" + layer + "-" + layerIndex;
                    oldVertexEdgesIter.put(newV, graph.outgoingEdgesOf(ov).iterator());
                } else {
                    String[] vCutFromEnd = oldVertexIter.next();
                    String parentId = graph
                            .getEdgeSource(
                                    graph.incomingEdgesOf(String.join("-", vCutFromEnd)).iterator().next()
                            )
                            .split("-")[0];
                    changes.put(vCutFromEnd[0], new ControlConfiguration()
                            .setDisconnect(parentId)
                            .setReconnect(v.split("-")[0])
                            .setMeta(id, layer, layerIndex));
                    newV = vCutFromEnd[0] + "-" + id + "-" + layer + "-" + layerIndex;
                }
                temp.add(newV);
                newGraph.addVertex(newV);
                newGraph.addEdge(v, newV);
                id++;
                layerIndex++;
            }
            nodeArray.addAll(temp);
        }


        return new ControlMsg(newGraph, ModelConstants.ActiveSacleUp, changes);
    }

}
