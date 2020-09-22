package org.apache.whale.multicast.core;

import org.apache.commons.io.IOUtils;
import org.jgrapht.Graph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.io.*;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

/**
 * locate org.apache.storm.multicast.model
 * Created by master on 2019/10/13.
 */
public class DirectedGraphJsonTransformTest {

    @Test
    public void testGraphJsonTransform() throws ExportException, ImportException {
        /**
         * <pre>
         *          1
         *        |    |
         *        2    3
         *          |
         *          4
         *          |
         *          7
         * </pre>
         */
        Graph<Integer, DefaultEdge> g = new DirectedAcyclicGraph<Integer, DefaultEdge>(DefaultEdge.class);
        g.addVertex(1);
        g.addVertex(2);
        g.addVertex(3);
        g.addVertex(4);
        g.addVertex(7);

        g.addEdge(1, 2);
        g.addEdge(1, 3);
        g.addEdge(2, 4);
        g.addEdge(3, 4);
        g.addEdge(4, 7);

        ComponentNameProvider<Integer> vertexIdProvider = new ComponentNameProvider<Integer>() {
            public String getName(Integer i) {
                return String.valueOf(i);
            }
        };

        GraphExporter<Integer, DefaultEdge> exporter =
                new JSONExporter<Integer, DefaultEdge>(vertexIdProvider);
        Writer writer = new StringWriter();
        exporter.exportGraph(g, writer);
        IOUtils.closeQuietly(writer);
        String json = writer.toString();
        System.out.println(json);
        // {"creator":"JGraphT JSON
        // Exporter","version":"1","nodes":[{"id":"1"},{"id":"2"},{"id":"3"},{"id":"4"},{"id":"7"}],"edges":[{"id":"1","source":"1","target":"2"},{"id":"2","source":"1","target":"3"},{"id":"3","source":"2","target":"4"},{"id":"4","source":"3","target":"4"},{"id":"5","source":"4","target":"7"}]}

        VertexProvider<Integer> vertexProvider = new VertexProvider<Integer>() {

            @Override
            public Integer buildVertex(String id, Map<String, Attribute> attributes) {
                System.out.println(id);
                return Integer.valueOf(id);
            }
        };
        EdgeProvider<Integer, DefaultEdge> edgeProvider = new EdgeProvider<Integer, DefaultEdge>() {

            @Override
            public DefaultEdge buildEdge(Integer from, Integer to, String label,
                                         Map<String, Attribute> attributes) {
                System.out.println(from+" "+to);
                Graph<Integer, DefaultEdge> tmpGraph =
                        new DirectedAcyclicGraph<Integer, DefaultEdge>(DefaultEdge.class);
                tmpGraph.addVertex(from);
                tmpGraph.addVertex(to);
                tmpGraph.addEdge(from, to);
                return tmpGraph.edgeSet().iterator().next();
            }
        };

        GraphImporter<Integer, DefaultEdge> importer =
                new JSONImporter<Integer, DefaultEdge>(vertexProvider, edgeProvider);

        Graph<Integer, DefaultEdge> g1 = new DirectedAcyclicGraph<Integer, DefaultEdge>(DefaultEdge.class);
        Reader reader = new StringReader(json);
        importer.importGraph(g1, reader);
        IOUtils.closeQuietly(reader);
        System.out.println(g1);
        // ([1, 2, 3, 4, 7], [(1,2), (1,3), (2,4), (3,4), (4,7)])
    }
}
