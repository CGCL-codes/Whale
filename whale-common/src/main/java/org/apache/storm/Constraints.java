package org.apache.storm;


import org.apache.storm.utils.PropertiesUtil;

/**
 * locate com.basic.storm
 * Created by tj on 2017/5/12.
 */
public class Constraints {
    static {
        PropertiesUtil.init("/whale.properties");
    }

    // Whale-Benchmark
    public static final String THROUGHPUT_STREAM_ID = "throughputstream";
    public static final String THROUGHPUT_STREAM_COUNT = "throughputCount";
    public static final String LATENCY_STREAM_ID = "latenyctream";
    public static final String LATENCY_STREAM_AVGLATENCY = "avgDelay";
    public static final String TIME_INFO = "timeinfo";
    public static final String TASK_ID = "taskid";
    public static final String START_TIME_MILLS = "startTimeMills";

    // Whale-Benchmark-Bolt
    public static final String THROUGHPUT_BOLT_ID ="throughput-bolt";
    public static final String BENCH_BOLT_ID ="bench-bolt";
    public static final String LATENCY_BOLT_ID ="latency-bolt";

    // Whale-Bolt
    public static final String DIDIMATCH_BOLT_ID ="didiMatch-bolt";
    public static final String STOCKDEAL_BOLT_ID ="stockDeal-bolt";
    public static final String RANDOM_SPOTU_ID ="random-spout";
    public static final String DELAY_BOLT_ID ="delay-bolt";
    public static final String GATHER_BOLT_ID ="gather-bolt";
    public static final String FORWARD_BOLT_ID="forward-bolt";
    public static final String BROADCAST_BOLT="broadcastbolt";

    // Whale-Spout
    public static final String KAFKA_SPOUT_ID ="kafka-spout";
    public static final String RDNAOM_SPOUT_ID ="random-spout";

    // Whale-Stream ID
    public static final String SPOUT_STREAM_ID ="spout_stream";
    public static final String BROADCAST_STREAM_ID ="broadcaststream";
    public static final String ACKCOUNT_STREAM_ID="ackcountstream";
    private static final String GATHER_STREAM_ID ="gahterspoutstream";
    public static final String FORWARD_STREAM_ID = "forwadrstream";

    // Kafka Configuration
    public static final String KAFKA_LOCAL_BROKER = PropertiesUtil.getProperties("KAFKA_LOCAL_BROKER");

    // Mysql Configuration
    public static final String C3P0_DATABASE_CONFIG = PropertiesUtil.getProperties("C3P0_DATABASE_CONFIG");

    // DirectSchudler
    public static final String SCHEDULER_SPECIAL_SUPERVISOR="special-supervisor";

    // DiDiOrderMatch Line
    public static final int DIDI_MATCH_LINE = Integer.parseInt(PropertiesUtil.getProperties("DIDI_MATCH_LINE"));;
    public static final int DIDI_MATCH_LATECNY = Integer.parseInt(PropertiesUtil.getProperties("DIDI_MATCH_LATECNY"));; //ns

    // StockDeal Line
    public static final int STOCKDEAL_MATCH_LINE = Integer.parseInt(PropertiesUtil.getProperties("STOCKDEAL_MATCH_LINE"));;
    public static final int STOCKDEAL_MATCH_LATECNY = Integer.parseInt(PropertiesUtil.getProperties("STOCKDEAL_MATCH_LATECNY"));; //ns

    // Whale-Benchmark Configuration
    public static final String BOLT_INSTANCES_NUM = "boltInstancesNum";
    public static final String TOPOLOGY_WORKERS_NUM = "topologyWorkersNum";
    public static final String BOLT_MAXINSTANCE_NUM = "boltMaxInstancesNum";
}
