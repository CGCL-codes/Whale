package org.apache.storm.multicast.model;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.junit.Test;

import java.util.Iterator;

/**
 * locate org.apache.storm
 * Created by MasterTj on 2019/10/11.
 */
public class DirectedAcyclicGraphTest {
    @Test
    public void testirectedAcyclicGraph() {
        DirectedAcyclicGraph<String,DefaultEdge> dag = createGraph();
        System.out.println(dag);

        BreadthFirstIterator<String, DefaultEdge> bfi = new BreadthFirstIterator<String, DefaultEdge>(dag, "v1");
        while (bfi.hasNext()) {
            System.out.println( bfi.next() );
        }
        System.out.println();

        // Topology 拓扑遍历
        Iterator<String> topologyIterator = dag.iterator();
        while (topologyIterator.hasNext()){
            System.out.println( topologyIterator.next() );
        }
        System.out.println();

        // Vertex 插入顺序遍历
        Iterator<String> sequentialIterator = dag.vertexSet().iterator();
        while (sequentialIterator.hasNext()){
            System.out.println( sequentialIterator.next() );
        }
        System.out.println();
    }

    private static DirectedAcyclicGraph<String,DefaultEdge> createGraph(){
        DirectedAcyclicGraph<String,DefaultEdge> g = new DirectedAcyclicGraph<String,DefaultEdge>(DefaultEdge.class);
        String v1 = "v1";
        String v2 = "v2";
        String v3 = "v3";
        String v4 = "v4";
        String v5 = "v5";

        // add the vertices
        g.addVertex(v1);
        g.addVertex(v2);
        g.addVertex(v3);
        g.addVertex(v4);
        g.addVertex(v5);

        // add edges to create a circuit
        try {
            g.addEdge(v1, v5);
            g.addEdge(v5, v3);
            g.addEdge(v3, v4);
            g.addEdge(v1, v2);
            g.addEdge(v4, v2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return g;
    }
}
