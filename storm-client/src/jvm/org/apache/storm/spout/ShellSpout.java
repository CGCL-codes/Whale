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
package org.apache.storm.spout;

import org.apache.storm.Config;
import org.apache.storm.generated.ShellComponent;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.rpc.IShellMetric;
import org.apache.storm.multilang.ShellMsg;
import org.apache.storm.multilang.SpoutMsg;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ShellLogHandler;
import org.apache.storm.utils.ShellProcess;
import org.apache.storm.utils.ShellUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ShellSpout implements ISpout {
    public static final Logger LOG = LoggerFactory.getLogger(ShellSpout.class);
    private static final long serialVersionUID = 5982357019665454L;

    private SpoutOutputCollector _collector;
    private String[] _command;
    private Map<String, String> env = new HashMap<>();
    private ShellLogHandler _logHandler;
    private ShellProcess _process;
    private volatile boolean _running = true;
    private volatile RuntimeException _exception;

    private TopologyContext _context;
    
    private SpoutMsg _spoutMsg;

    private int workerTimeoutMills;
    private ScheduledExecutorService heartBeatExecutorService;
    private AtomicLong lastHeartbeatTimestamp = new AtomicLong();
    private AtomicBoolean waitingOnSubprocess = new AtomicBoolean(false);
    private boolean changeDirectory = true;

    public ShellSpout(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }

    public ShellSpout(String... command) {
        _command = command;
    }

    public ShellSpout setEnv(Map<String, String> env) {
        this.env = env;
        return this;
    }

    public boolean shouldChangeChildCWD() {
        return changeDirectory;
    }

    /**
     * Set if the current working directory of the child process should change
     * to the resources dir from extracted from the jar, or if it should stay
     * the same as the worker process to access things from the blob store.
     * @param changeDirectory true change the directory (default) false
     * leave the directory the same as the worker process.
     */
    public void changeChildCWD(boolean changeDirectory) {
        this.changeDirectory = changeDirectory;
    }

    public void open(Map<String, Object> topoConf, TopologyContext context,
                     SpoutOutputCollector collector) {
        _collector = collector;
        _context = context;

        if (topoConf.containsKey(Config.TOPOLOGY_SUBPROCESS_TIMEOUT_SECS)) {
            workerTimeoutMills = 1000 * ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_SUBPROCESS_TIMEOUT_SECS));
        } else {
            workerTimeoutMills = 1000 * ObjectReader.getInt(topoConf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS));
        }

        _process = new ShellProcess(_command);
        if (!env.isEmpty()) {
            _process.setEnv(env);
        }

        Number subpid = _process.launch(topoConf, context, changeDirectory);
        LOG.info("Launched subprocess with pid " + subpid);

        _logHandler =  ShellUtils.getLogHandler(topoConf);
        _logHandler.setUpContext(ShellSpout.class, _process, _context);

        heartBeatExecutorService = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));
    }

    public void close() {
        heartBeatExecutorService.shutdownNow();
        _process.destroy();
        _running = false;
    }

    public void nextTuple() {
        this.sendSyncCommand("next", "");
    }

    public void ack(Object msgId) {
        this.sendSyncCommand("ack", msgId);
    }

    public void fail(Object msgId) {
        this.sendSyncCommand("fail", msgId);
    }

    private void sendSyncCommand(String command, Object msgId) {
        if (_exception != null) {
            throw _exception;
        }

        if (_spoutMsg == null) {
            _spoutMsg = new SpoutMsg();
        }
        _spoutMsg.setCommand(command);
        _spoutMsg.setId(msgId);
        querySubprocess();
    }

    
    private void handleMetrics(ShellMsg shellMsg) {
        //get metric name
        String name = shellMsg.getMetricName();
        if (name.isEmpty()) {
            throw new RuntimeException("Receive Metrics name is empty");
        }
        
        //get metric by name
        IMetric iMetric = _context.getRegisteredMetricByName(name);
        if (iMetric == null) {
            throw new RuntimeException("Could not find metric by name["+name+"] ");
        }
        if ( !(iMetric instanceof IShellMetric)) {
            throw new RuntimeException("Metric["+name+"] is not IShellMetric, can not call by RPC");
        }
        IShellMetric iShellMetric = (IShellMetric)iMetric;
        
        //call updateMetricFromRPC with params
        Object paramsObj = shellMsg.getMetricParams();
        try {
            iShellMetric.updateMetricFromRPC(paramsObj);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }       
    }

    private void querySubprocess() {
        try {
            markWaitingSubprocess();
            _process.writeSpoutMsg(_spoutMsg);

            while (true) {
                ShellMsg shellMsg = _process.readShellMsg();
                String command = shellMsg.getCommand();
                if (command == null) {
                    throw new IllegalArgumentException("Command not found in spout message: " + shellMsg);
                }

                setHeartbeat();

                if (command.equals("sync")) {
                    return;
                } else if (command.equals("log")) {
                    _logHandler.log(shellMsg);
                } else if (command.equals("error")) {
                    handleError(shellMsg.getMsg());
                } else if (command.equals("emit")) {
                    String stream = shellMsg.getStream();
                    Long task = shellMsg.getTask();
                    List<Object> tuple = shellMsg.getTuple();
                    Object messageId = shellMsg.getId();
                    if (task == 0) {
                        List<Integer> outtasks = _collector.emit(stream, tuple, messageId);
                        if (shellMsg.areTaskIdsNeeded()) {
                            _process.writeTaskIds(outtasks);
                        }
                    } else {
                        _collector.emitDirect((int) task.longValue(), stream, tuple, messageId);
                    }
                } else if (command.equals("metrics")) {
                    handleMetrics(shellMsg);
                } else {
                    throw new RuntimeException("Unknown command received: " + command);
                }
            }
        } catch (Exception e) {
            String processInfo = _process.getProcessInfoString() + _process.getProcessTerminationInfoString();
            throw new RuntimeException(processInfo, e);
        } finally {
            completedWaitingSubprocess();
        }
    }

    private void handleError(String msg) {
        _collector.reportError(new Exception("Shell Process Exception: " + msg));
    }

    @Override
    public void activate() {
        LOG.info("Start checking heartbeat...");
        // prevent timer to check heartbeat based on last thing before activate
        setHeartbeat();
        if (heartBeatExecutorService.isShutdown()){
            //In case deactivate was called before
            heartBeatExecutorService = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));
        }
        heartBeatExecutorService.scheduleAtFixedRate(new SpoutHeartbeatTimerTask(this), 1, 1, TimeUnit.SECONDS);
        this.sendSyncCommand("activate", "");
    }

    @Override
    public void deactivate() {
        this.sendSyncCommand("deactivate", "");
        heartBeatExecutorService.shutdownNow();
    }

    private void setHeartbeat() {
        lastHeartbeatTimestamp.set(System.currentTimeMillis());
    }

    private long getLastHeartbeat() {
        return lastHeartbeatTimestamp.get();
    }

    private void markWaitingSubprocess() {
        setHeartbeat();
        waitingOnSubprocess.compareAndSet(false, true);
    }

    private void completedWaitingSubprocess() {
        waitingOnSubprocess.compareAndSet(true, false);
    }

    private void die(Throwable exception) {
        String processInfo = _process.getProcessInfoString() + _process.getProcessTerminationInfoString();
        _exception = new RuntimeException(processInfo, exception);
        String message = String.format("Halting process: ShellSpout died. Command: %s, ProcessInfo %s",
            Arrays.toString(_command),
            processInfo);
        LOG.error(message, exception);
        _collector.reportError(exception);
        if (_running || (exception instanceof Error)) { //don't exit if not running, unless it is an Error
            System.exit(11);
        }
    }

    private class SpoutHeartbeatTimerTask extends TimerTask {
        private ShellSpout spout;

        public SpoutHeartbeatTimerTask(ShellSpout spout) {
            this.spout = spout;
        }

        @Override
        public void run() {
            long lastHeartbeat = getLastHeartbeat();
            long currentTimestamp = System.currentTimeMillis();
            boolean isWaitingOnSubprocess = waitingOnSubprocess.get();

            LOG.debug("last heartbeat : {}, waiting subprocess now : {}, worker timeout (ms) : {}",
                    lastHeartbeat, isWaitingOnSubprocess, workerTimeoutMills);

            if (isWaitingOnSubprocess && currentTimestamp - lastHeartbeat > workerTimeoutMills) {
                spout.die(new RuntimeException("subprocess heartbeat timeout"));
            }
        }
    }

}
