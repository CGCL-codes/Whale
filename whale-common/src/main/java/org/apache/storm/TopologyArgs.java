package org.apache.storm;

import org.apache.storm.util.TimeUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newLinkedList;
import static org.apache.commons.io.FilenameUtils.normalizeNoEndSeparator;

public class TopologyArgs implements Serializable
{
    private static Logger logger= LoggerFactory.getLogger(TopologyArgs.class);

    private static final int SCREEN_WIDTH = 80;

    private final String _mainclassName;

    public TopologyArgs(String className) {
        _mainclassName = className;
    }

    // Kafka Configuration
    private static final String DEFAULT_KAFKA_TOPIC = "defaultTopic";

    // Storm Topology Configuration
    private static final String DEFAULT_TOPOLOGY_NAME = "Whale-Multicast";
    private static final boolean DEFAULT_REMOTE_MODE = true;
    private static final boolean DEFAULT_DEBUG = true;
    private static final int DEFAULT_NUM_WORKER = 5;
    private static final int DEFAULT_NUM_KAFKASPOUT = 1;
    private static final int DEFAULT_NUM_MULTICASTBOLT = 4;

    // Stream Multicast Model Configuration
    private static final int DEFAULT_MAX_OUTDEGREE = 4;
    private static final int DEFAULT_POSSION_EXPECT = 200000;

    // RDMA Configuration
    private static final String DEFAULT_NETWORK_TRANSPORT = "netty";
    private static final int DEFAULT_SENDTIMELIMIT = 1;
    private static final int DEFAULT_MAXMEMORY_SIZE = 262144;

    @Option(name = "-topic", aliases = "--kafka-topic", metaVar = "<str>",
            usage = "topic of Kafka [def:\"" + DEFAULT_KAFKA_TOPIC + "\"]")
    public String kafkaTopic = DEFAULT_KAFKA_TOPIC;

    @Option(name = "-topology", aliases = "--topology-name", metaVar = "<str>",
            usage = "Storm TopologyName [def:\"" + DEFAULT_TOPOLOGY_NAME + "\"]")
    public String topologyName = DEFAULT_TOPOLOGY_NAME;

    @Option(name = "-lrt", aliases = "--local-runtime", metaVar = "<num>",
            usage = "running time (in seconds) for local mode [def:5]")
    public int localRuntime = 5;

    @Option(name = "--remote",
            usage = "run topology in cluster (remote mode) [def:true]")
    public boolean remoteMode = DEFAULT_REMOTE_MODE;

    @Option(name = "--debug", usage = "enable debug logs [def:false]")
    public boolean debug = DEFAULT_DEBUG;

    @Option(name = "-nw", aliases = "--num-worker", metaVar = "<num>",
            usage = "# storm topology workers number [def:"+DEFAULT_NUM_WORKER+"]")
    public int numWorkers = DEFAULT_NUM_WORKER;

    @Option(name = "-nks", aliases = "--num-kafkaspout", metaVar = "<num>",
            usage = "# the number of kafka spout [def:"+DEFAULT_NUM_KAFKASPOUT+"]")
    public int numKafkaSpouts = DEFAULT_NUM_KAFKASPOUT;

    @Option(name = "-nmb", aliases = "--num-multicastbolt", metaVar = "<num>",
            usage = "# the number of multicast bolt [def:"+DEFAULT_NUM_MULTICASTBOLT+"]")
    public int numMulticastBolts = DEFAULT_NUM_MULTICASTBOLT;

    @Option(name = "-md", aliases = "--max-outdegree", metaVar = "<num>",
            usage = "# the max outDegree in balanced parital multicast model [def:" +DEFAULT_MAX_OUTDEGREE+ "]")
    public int maxOutDegree = DEFAULT_MAX_OUTDEGREE;

    @Option(name = "-pe", aliases = "--possion-expect", metaVar = "<num>",
            usage = "# spout output rate in KafkaPoissonSpout [def:" + DEFAULT_POSSION_EXPECT+ "]")
    public int possionExpect = DEFAULT_POSSION_EXPECT;

    @Option(name = "-nt", aliases = "--network-transport", metaVar = "<str>",
            usage = "network transport(TCP/RDMA) in Whale [def:\"" + DEFAULT_NETWORK_TRANSPORT + "\"]")
    public String networkTransport = DEFAULT_NETWORK_TRANSPORT;

    @Option(name = "-stl", aliases = "--", metaVar = "<num>",
            usage = "# sendTimeLimit(ms) in RDMA-assisted transmission mechanism [def:" + DEFAULT_SENDTIMELIMIT+ "]")
    public int sendTimeLimit = DEFAULT_SENDTIMELIMIT;

    @Option(name = "-mms", aliases = "---s", metaVar = "<num>",
            usage = "# maxMemorySize(bytes) in RDMA-assisted transmission mechanism [def:" + DEFAULT_MAXMEMORY_SIZE+ "]")
    public int maxMemorySize = DEFAULT_MAXMEMORY_SIZE;

    @Option(name = "--no-output",
            usage = "disable output of join results [def:false]")
    public Boolean noOutput = false;

    @Option(name = "-d", aliases = "--output-dir", metaVar = "<path>",
            usage = "output directory [def:null]")
    public String outputDir = null;

    @Option(name = "-h", aliases = { "-?", "--help" }, hidden = false,
            help = true, usage = "print this help message")
    public boolean help = false;

    public boolean processArgs(String[] args) throws Exception {
        if (!parseArgs(args))
            return false;

        if (help) {
            printHelp(System.out);
        } else {
            sanityCheckArgs();
            deriveArgs();
        }

        return true;
    }

    private boolean parseArgs(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        parser.getProperties().withUsageWidth(SCREEN_WIDTH);

        try {
            parser.parseArgument(args);
        }
        catch (CmdLineException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println();
            printHelp(System.err);

            return false;
        }

        return true;
    }

    private void sanityCheckArgs() {

        // Storm Topology Configuration
        checkState(numWorkers > 0, "At least one worker is required");
        checkState(numKafkaSpouts > 0, "At least one kafka spout is required");
        checkState(numMulticastBolts > 0, "At least one multicast bolt is required");

        // Stream Multicast Model Configuration
        checkState(maxOutDegree > 0, "maxOutDegree cannot be negative");
        checkState(possionExpect > 0, "possionExpect maxOutDegree cannot be negative");

        // RDMA Configuration
        checkState(sendTimeLimit > 0, "sendTimeLimit maxOutDegree cannot be negative");
        checkState(maxMemorySize > 0, "maxMemorySize maxOutDegree cannot be negative");
    }

    private void deriveArgs() {
        if (outputDir != null) {
            outputDir = normalizeNoEndSeparator(outputDir);
            if (outputDir.isEmpty()) {
                outputDir = ".";
            }
            else {
                outputDir += "_" + TimeUtils.getTimestamp();
            }
        }
    }

    public void showArgs() {
        for (String msg : getInfo()) {
            System.out.println(msg);
        }
    }

    public void logArgs() {
        for (String msg : getInfo()) {
            logger.info(msg);
        }
    }

    public void logArgs(FileWriter output) throws IOException {
        for (String msg : getInfo()) {
            output.write(msg);
        }
    }

    private List<String> getInfo() {
        List<String> info = newLinkedList();

        // Kafka Configuration
        info.add("  kafka topic name: " + kafkaTopic);

        // Storm Topology Configuration
        info.add("  topology name: " + topologyName);
        info.add("  remote mode: " + remoteMode);
        info.add("  local runtime: " + localRuntime + " sec");
        info.add("  debug: " + debug);
        info.add("  workers: " + numWorkers);
        info.add("  numKafkaSpouts: " + numKafkaSpouts);
        info.add("  numMulticastBolts: " + numMulticastBolts);

        // Stream Multicast Model Configuration
        info.add("  maxOutDegree: " + maxOutDegree);
        info.add("  possionExpect: " + possionExpect);

        // RDMA Configuration
        info.add("  networkTransport: " + networkTransport);
        info.add("  sendTimeLimit: " + sendTimeLimit);
        info.add("  maxMemorySize: " + maxMemorySize);

        info.add("  output: " + !noOutput);
        if (outputDir != null) {
            info.add("  output dir: " + outputDir);
        }

        return info;
    }

    private void printHelp(PrintStream out) {
        String cmd = "java " + _mainclassName;
        String cmdIndent = "  " + cmd;

        out.println("USAGE: ");
        out.println(cmdIndent + " [OPTION]...");
        out.println();
        out.println("OPTIONS: ");
        (new CmdLineParser(this)).printUsage(out);
        out.println();
    }

    public static void main(String[] args) throws Exception {
        TopologyArgs allArgs = new TopologyArgs("TopologyArgs");

        if (!allArgs.processArgs(args))
            return ;

        if (!allArgs.help)
            allArgs.showArgs();
    }
}
