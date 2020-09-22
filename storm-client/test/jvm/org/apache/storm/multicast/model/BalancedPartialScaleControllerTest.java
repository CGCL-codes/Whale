package org.apache.storm.multicast.model;

import org.apache.storm.multicast.dynamicswitch.ControlConfiguration;
import org.apache.storm.multicast.dynamicswitch.DynamicSwitchController;
import org.apache.storm.multicast.dynamicswitch.ControlMsg;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * locate org.apache.storm.multicast.model
 * Created by master on 2019/10/31.
 */
public class BalancedPartialScaleControllerTest {
    private static final int componentNumber = 15;

    @Test
    public void testBalancedPartialScaleUp() {
        DirectedAcyclicGraph<String, DefaultEdge> graph = createGraph(1);

        BreadthFirstIterator<String, DefaultEdge> bfi = new BreadthFirstIterator<>(graph, "component1-1-0-1");
        System.out.println("before dynamic switch: ");
        while (bfi.hasNext()) {
            System.out.print(bfi.next() + " ");
        }
        System.out.println("\n");

        long start = System.currentTimeMillis();
        ControlMsg newOne = DynamicSwitchController.change(graph, 1, 3);
        System.out.println("dynamic swtich time: "+ (System.currentTimeMillis() - start) +" ms \n");

        BreadthFirstIterator<String, DefaultEdge> bfi2 = new BreadthFirstIterator<>(newOne.getGraph(), "component1-1-0-1");
        System.out.println("after dynamic switch: ");
        while (bfi2.hasNext()) {
            System.out.print(bfi2.next() + " ");
        }
        System.out.println("\n");

        for (int i = 1; i <= componentNumber; i++) {
            ControlConfiguration t = newOne.getControlConfiguration().get("component" + i);
            if (t == null) continue;
            System.out.println("component"+ i + " : " + t.toString());
        }
//        String[] t = newOne.getGraph().vertexSet().stream()
//                .map(v -> v.split("-"))
//                .sorted((a, b) -> Integer.parseInt(b[1]) - Integer.parseInt(a[1]))
//                .map(v -> String.join("-", v))
//                .toArray(String[]::new);
//        System.out.println(Arrays.toString(t));

    }

    @Test
    public void testBalancedPartialScaleDown() {
        DirectedAcyclicGraph<String, DefaultEdge> graph = createGraph(3);

        BreadthFirstIterator<String, DefaultEdge> bfi = new BreadthFirstIterator<>(graph, "component1-1-0-1");
        System.out.println("before dynamic switch: ");
        while (bfi.hasNext()) {
            System.out.print(bfi.next() + " ");
        }
        System.out.println("\n");

        long start = System.currentTimeMillis();
        ControlMsg newOne = DynamicSwitchController.change(graph, 3, 1);
        System.out.println("dynamic swtich time: "+ (System.currentTimeMillis() - start) +" ms \n");
        System.out.println("\n");

        BreadthFirstIterator<String, DefaultEdge> bfi2 = new BreadthFirstIterator<>(newOne.getGraph(), "component1-1-0-1");
        System.out.println("after dynamic switch: ");
        while (bfi2.hasNext()) {
            System.out.print(bfi2.next() + " ");
        }
        System.out.println("\n");

        for (int i = 1; i <= componentNumber; i++) {
            ControlConfiguration t = newOne.getControlConfiguration().get("component" + i);
            if (t == null) continue;
            System.out.println("component"+ i + " : " + t.toString());
        }
//        String[] t = newOne.getGraph().vertexSet().stream()
//                .map(v -> v.split("-"))
//                .sorted((a, b) -> Integer.parseInt(b[1]) - Integer.parseInt(a[1]))
//                .map(v -> String.join("-", v))
//                .toArray(String[]::new);
//        System.out.println(Arrays.toString(t));

    }

    private static DirectedAcyclicGraph<String, DefaultEdge> createGraph(int degree) {
        ArrayList<String> nodeArray = new ArrayList<>();
        DirectedAcyclicGraph<String, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);
        // componentId-id-layer-index;
        nodeArray.add("component1-1-0-1");
        graph.addVertex(nodeArray.get(0));

        int index = 2;
        for (int layer = 1; index <= componentNumber; layer++) {
            int layerIndex = 1;
            Iterator<String> iter = nodeArray.iterator();
            ArrayList<String> temp = new ArrayList<>();
            while (iter.hasNext()) {
                if (index > componentNumber)
                    break;
                String v = iter.next();
                if (graph.outDegreeOf(v) == degree)
                    iter.remove();
                else {
                    String newV = "component" + index + "-" + index + "-" + layer + "-" + layerIndex;
                    temp.add(newV);
                    graph.addVertex(newV);
                    graph.addEdge(v, newV);
                    layerIndex++;
                    index++;
                }
            }
            nodeArray.addAll(temp);
        }
        return graph;
    }
}
