/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hive.bolt;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.SerializationError;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.Config;
import org.apache.storm.hive.common.HiveWriter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.hive.common.HiveUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.List;
import java.io.IOException;

public class HiveBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(HiveBolt.class);
    private OutputCollector collector;
    private HiveOptions options;
    private ExecutorService callTimeoutPool;
    private transient Timer heartBeatTimer;
    private AtomicBoolean sendHeartBeat = new AtomicBoolean(false);
    private UserGroupInformation ugi = null;
    private Map<HiveEndPoint, HiveWriter> allWriters;
    private BatchHelper batchHelper;
    private boolean tokenAuthEnabled;

    public HiveBolt(HiveOptions options) {
        this.options = options;
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext topologyContext, OutputCollector collector)  {
        try {
            tokenAuthEnabled = HiveUtils.isTokenAuthEnabled(conf);
            try {
                ugi = HiveUtils.authenticate(tokenAuthEnabled, options.getKerberosKeytab(), options.getKerberosPrincipal());
            } catch(HiveUtils.AuthenticationFailed ex) {
                LOG.error("Hive kerberos authentication failed " + ex.getMessage(), ex);
                throw new IllegalArgumentException(ex);
            }

            this.collector = collector;
            this.batchHelper = new BatchHelper(options.getBatchSize(), collector);
            allWriters = new ConcurrentHashMap<HiveEndPoint,HiveWriter>();
            String timeoutName = "hive-bolt-%d";
            this.callTimeoutPool = Executors.newFixedThreadPool(1,
                                new ThreadFactoryBuilder().setNameFormat(timeoutName).build());

            sendHeartBeat.set(true);
            heartBeatTimer = new Timer();
            setupHeartBeatTimer();

        } catch(Exception e) {
            LOG.warn("unable to make connection to hive ", e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (batchHelper.shouldHandle(tuple)) {
                List<String> partitionVals = options.getMapper().mapPartitions(tuple);
                HiveEndPoint endPoint = HiveUtils.makeEndPoint(partitionVals, options);
                HiveWriter writer = getOrCreateWriter(endPoint);
                writer.write(options.getMapper().mapRecord(tuple));
                batchHelper.addBatch(tuple);
            }

            if(batchHelper.shouldFlush()) {
                flushAllWriters(true);
                LOG.info("acknowledging tuples after writers flushed ");
                batchHelper.ack();
            }
            if (TupleUtils.isTick(tuple)) {
                retireIdleWriters();
            }
        } catch(SerializationError se) {
            LOG.info("Serialization exception occurred, tuple is acknowledged but not written to Hive.", tuple);
            this.collector.reportError(se);
            collector.ack(tuple);
        } catch(Exception e) {
            batchHelper.fail(e);
            abortAndCloseWriters();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        sendHeartBeat.set(false);
        for (Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            try {
                HiveWriter w = entry.getValue();
                w.flushAndClose();
            } catch (Exception ex) {
                LOG.warn("Error while closing writer to " + entry.getKey() +
                         ". Exception follows.", ex);
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        ExecutorService toShutdown[] = {callTimeoutPool};
        for (ExecutorService execService : toShutdown) {
            execService.shutdown();
            try {
                while (!execService.isTerminated()) {
                    execService.awaitTermination(
                                 options.getCallTimeOut(), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException ex) {
                LOG.warn("shutdown interrupted on " + execService, ex);
            }
        }

        callTimeoutPool = null;
        super.cleanup();
        LOG.info("Hive Bolt stopped");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = super.getComponentConfiguration();
        if (conf == null)
            conf = new Config();

        if (options.getTickTupleInterval() > 0)
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, options.getTickTupleInterval());

        return conf;
    }

    private void setupHeartBeatTimer() {
        if(options.getHeartBeatInterval()>0) {
            heartBeatTimer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            if (sendHeartBeat.get()) {
                                LOG.debug("Start sending heartbeat on all writers");
                                sendHeartBeatOnAllWriters();
                                setupHeartBeatTimer();
                            }
                        } catch (Exception e) {
                            LOG.warn("Failed to heartbeat on HiveWriter ", e);
                        }
                    }
                }, options.getHeartBeatInterval() * 1000);
        }
    }

    private void sendHeartBeatOnAllWriters() throws InterruptedException {
        for (HiveWriter writer : allWriters.values()) {
            writer.heartBeat();
        }
    }

    void flushAllWriters(boolean rollToNext)
        throws HiveWriter.CommitFailure, HiveWriter.TxnBatchFailure, HiveWriter.TxnFailure, InterruptedException {
        for(HiveWriter writer: allWriters.values()) {
            writer.flush(rollToNext);
        }
    }

    void abortAndCloseWriters() {
        try {
            abortAllWriters();
            closeAllWriters();
        }  catch(Exception ie) {
            LOG.warn("unable to close hive connections. ", ie);
        }
    }

    /**
     * Abort current Txn on all writers
     */
    private void abortAllWriters() throws InterruptedException, StreamingException, HiveWriter.TxnBatchFailure {
        for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
            try {
                entry.getValue().abort();
            } catch (Exception e) {
                LOG.error("Failed to abort hive transaction batch, HiveEndPoint " + entry.getValue() +" due to exception ", e);
            }
        }
    }

    /**
     * Closes all writers and remove them from cache
     */
    private void closeAllWriters() {
        //1) Retire writers
        for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
            try {
                entry.getValue().close();
            } catch(Exception e) {
                LOG.warn("unable to close writers. ", e);
            }
        }
        //2) Clear cache
        allWriters.clear();
    }

    private HiveWriter getOrCreateWriter(HiveEndPoint endPoint)
        throws HiveWriter.ConnectFailure, InterruptedException {
        try {
            HiveWriter writer = allWriters.get( endPoint );
            if (writer == null) {
                LOG.debug("Creating Writer to Hive end point : " + endPoint);
                writer = HiveUtils.makeHiveWriter(endPoint, callTimeoutPool, ugi, options, tokenAuthEnabled);
                if (allWriters.size() > (options.getMaxOpenConnections() - 1)) {
                    LOG.info("cached HiveEndPoint size {} exceeded maxOpenConnections {} ", allWriters.size(), options.getMaxOpenConnections());
                    int retired = retireIdleWriters();
                    if(retired==0) {
                        retireEldestWriter();
                    }
                }
                allWriters.put(endPoint, writer);
                HiveUtils.logAllHiveEndPoints(allWriters);
            }
            return writer;
        } catch (HiveWriter.ConnectFailure e) {
            LOG.error("Failed to create HiveWriter for endpoint: " + endPoint, e);
            throw e;
        }
    }

    /**
     * Locate writer that has not been used for longest time and retire it
     */
    private void retireEldestWriter() {
        LOG.info("Attempting close eldest writers");
        long oldestTimeStamp = System.currentTimeMillis();
        HiveEndPoint eldest = null;
        for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
            if (entry.getValue().getLastUsed() < oldestTimeStamp) {
                eldest = entry.getKey();
                oldestTimeStamp = entry.getValue().getLastUsed();
            }
        }
        try {
            LOG.info("Closing least used Writer to Hive end point : " + eldest);
            allWriters.remove(eldest).flushAndClose();
        } catch (IOException e) {
            LOG.warn("Failed to close writer for end point: " + eldest, e);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted when attempting to close writer for end point: " + eldest, e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Interrupted when attempting to close writer for end point: " + eldest, e);
        }
    }

    /**
     * Locate all writers past idle timeout and retire them
     * @return number of writers retired
     */
    private int retireIdleWriters() {
        LOG.info("Attempting close idle writers");
        int count = 0;
        long now = System.currentTimeMillis();

        //1) Find retirement candidates
        for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
            if(now - entry.getValue().getLastUsed() > options.getIdleTimeout()) {
                ++count;
                retire(entry.getKey());
            }
        }
        return count;
    }

    private void retire(HiveEndPoint ep) {
        try {
            HiveWriter writer = allWriters.remove(ep);
            if (writer != null) {
                LOG.info("Closing idle Writer to Hive end point : {}", ep);
                writer.flushAndClose();
            }
        } catch (IOException e) {
            LOG.warn("Failed to close writer for end point: {}. Error: " + ep, e);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted when attempting to close writer for end point: " + ep, e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("Interrupted when attempting to close writer for end point: " + ep, e);
        }
    }

}
