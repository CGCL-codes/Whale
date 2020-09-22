/*
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

package org.apache.storm.daemon.supervisor;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.storm.DaemonConfig;
import org.apache.storm.StormTimer;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.DaemonCommon;
import org.apache.storm.daemon.supervisor.timer.SupervisorHealthCheck;
import org.apache.storm.daemon.supervisor.timer.SupervisorHeartbeat;
import org.apache.storm.event.EventManager;
import org.apache.storm.event.EventManagerImp;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.localizer.AsyncLocalizer;
import org.apache.storm.messaging.IContext;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ServerConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.VersionInfo;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Supervisor implements DaemonCommon, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Supervisor.class);
    private final Map<String, Object> conf;
    private final IContext sharedContext;
    private volatile boolean active;
    private final ISupervisor iSupervisor;
    private final Utils.UptimeComputer upTime;
    private final String stormVersion;
    private final IStormClusterState stormClusterState;
    private final LocalState localState;
    private final String supervisorId;
    private final String assignmentId;
    private final String hostName;
    // used for reporting used ports when heartbeating
    private final AtomicReference<Map<Long, LocalAssignment>> currAssignment;
    private final StormTimer heartbeatTimer;
    private final StormTimer eventTimer;
    private final AsyncLocalizer asyncLocalizer;
    private EventManager eventManager;
    private ReadClusterState readState;
    
    private Supervisor(ISupervisor iSupervisor) throws IOException {
        this(Utils.readStormConfig(), null, iSupervisor);
    }
    
    public Supervisor(Map<String, Object> conf, IContext sharedContext, ISupervisor iSupervisor) throws IOException {
        this.conf = conf;
        this.iSupervisor = iSupervisor;
        this.active = true;
        this.upTime = Utils.makeUptimeComputer();
        this.stormVersion = VersionInfo.getVersion();
        this.sharedContext = sharedContext;
        
        iSupervisor.prepare(conf, ServerConfigUtils.supervisorIsupervisorDir(conf));
        
        List<ACL> acls = null;
        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
            acls = SupervisorUtils.supervisorZkAcls();
        }

        try {
            this.stormClusterState = ClusterUtils.mkStormClusterState(conf, acls, new ClusterStateContext(DaemonType.SUPERVISOR));
        } catch (Exception e) {
            LOG.error("supervisor can't create stormClusterState");
            throw Utils.wrapInRuntime(e);
        }

        this.currAssignment = new AtomicReference<>(new HashMap<>());

        try {
            this.localState = ServerConfigUtils.supervisorState(conf);
            this.asyncLocalizer = new AsyncLocalizer(conf);
        } catch (IOException e) {
            throw Utils.wrapInRuntime(e);
        }
        this.supervisorId = iSupervisor.getSupervisorId();
        this.assignmentId = iSupervisor.getAssignmentId();

        try {
            this.hostName = Utils.hostname();
        } catch (UnknownHostException e) {
            throw Utils.wrapInRuntime(e);
        }

        this.heartbeatTimer = new StormTimer(null, new DefaultUncaughtExceptionHandler());

        this.eventTimer = new StormTimer(null, new DefaultUncaughtExceptionHandler());
    }
    
    public String getId() {
        return supervisorId;
    }
    
    IContext getSharedContext() {
        return sharedContext;
    }

    public Map<String, Object> getConf() {
        return conf;
    }

    public ISupervisor getiSupervisor() {
        return iSupervisor;
    }

    public Utils.UptimeComputer getUpTime() {
        return upTime;
    }

    public String getStormVersion() {
        return stormVersion;
    }

    public IStormClusterState getStormClusterState() {
        return stormClusterState;
    }

    LocalState getLocalState() {
        return localState;
    }

    public String getAssignmentId() {
        return assignmentId;
    }

    public String getHostName() {
        return hostName;
    }

    public AtomicReference<Map<Long, LocalAssignment>> getCurrAssignment() {
        return currAssignment;
    }
    
    AsyncLocalizer getAsyncLocalizer() {
        return asyncLocalizer;
    }
    
    EventManager getEventManger() {
        return eventManager;
    }
    
    /**
     * Launch the supervisor
     */
    public void launch() throws Exception {
        LOG.info("Starting Supervisor with conf {}", conf);
        String path = ServerConfigUtils.supervisorTmpDir(conf);
        FileUtils.cleanDirectory(new File(path));

        SupervisorHeartbeat hb = new SupervisorHeartbeat(conf, this);
        hb.run();
        // should synchronize supervisor so it doesn't launch anything after being down (optimization)
        Integer heartbeatFrequency = ObjectReader.getInt(conf.get(DaemonConfig.SUPERVISOR_HEARTBEAT_FREQUENCY_SECS));
        heartbeatTimer.scheduleRecurring(0, heartbeatFrequency, hb);

        this.eventManager = new EventManagerImp(false);
        this.readState = new ReadClusterState(this);

        asyncLocalizer.start();

        if ((Boolean) conf.get(DaemonConfig.SUPERVISOR_ENABLE)) {
            // This isn't strictly necessary, but it doesn't hurt and ensures that the machine stays up
            // to date even if callbacks don't all work exactly right
            eventTimer.scheduleRecurring(0, 10, new EventManagerPushCallback(readState, eventManager));

            // supervisor health check
            eventTimer.scheduleRecurring(300, 300, new SupervisorHealthCheck(this));
        }
        LOG.info("Starting supervisor with id {} at host {}.", getId(), getHostName());
    }

    /**
     * start distribute supervisor
     */
    public void launchDaemon() {
        LOG.info("Starting supervisor for storm version '{}'.", VersionInfo.getVersion());
        try {
            Map<String, Object> conf = getConf();
            if (ConfigUtils.isLocalMode(conf)) {
                throw new IllegalArgumentException("Cannot start server in local mode!");
            }
            launch();
            Utils.addShutdownHookWithForceKillIn1Sec(() -> {this.close();});
            registerWorkerNumGauge("supervisor:num-slots-used-gauge", conf);
            StormMetricsRegistry.startMetricsReporters(conf);
        } catch (Exception e) {
            LOG.error("Failed to start supervisor\n", e);
            System.exit(1);
        }
    }

    private void registerWorkerNumGauge(String name, final Map<String, Object> conf) {
        StormMetricsRegistry.registerGauge(name, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                Collection<String> pids = SupervisorUtils.supervisorWorkerIds(conf);
                return pids.size();
            }
        });
    }
    
    @Override
    public void close() {
        try {
            LOG.info("Shutting down supervisor {}", getId());
            this.active = false;
            heartbeatTimer.close();
            eventTimer.close();
            if (eventManager != null) {
                eventManager.close();
            }
            if (readState != null) {
                readState.close();
            }
            asyncLocalizer.close();
            getStormClusterState().disconnect();
        } catch (Exception e) {
            LOG.error("Error Shutting down", e);
        }
    }
    
    void killWorkers(Collection<String> workerIds, ContainerLauncher launcher) throws InterruptedException, IOException {
        HashSet<Killable> containers = new HashSet<>();
        for (String workerId : workerIds) {
            try {
                Killable k = launcher.recoverContainer(workerId, localState);
                if (!k.areAllProcessesDead()) {
                    k.kill();
                    containers.add(k);
                } else {
                    k.cleanUp();
                }
            } catch (Exception e) {
                LOG.error("Error trying to kill {}", workerId, e);
            }
        }
        int shutdownSleepSecs = ObjectReader.getInt(conf.get(DaemonConfig.SUPERVISOR_WORKER_SHUTDOWN_SLEEP_SECS), 1);
        if (!containers.isEmpty()) {
            Time.sleepSecs(shutdownSleepSecs);
        }
        for (Killable k: containers) {
            try {
                k.forceKill();
                long start = Time.currentTimeMillis();
                while(!k.areAllProcessesDead()) {
                    if ((Time.currentTimeMillis() - start) > 10_000) {
                        throw new RuntimeException("Giving up on killing " + k 
                                + " after " + (Time.currentTimeMillis() - start) + " ms");
                    }
                    Time.sleep(100);
                    k.forceKill();
                }
                k.cleanUp();
            } catch (Exception e) {
                LOG.error("Error trying to clean up {}", k, e);
            }
        }
    }

    public void shutdownAllWorkers(UniFunc<Slot> onWarnTimeout, UniFunc<Slot> onErrorTimeout) {
        if (readState != null) {
            readState.shutdownAllWorkers(onWarnTimeout, onErrorTimeout);
        } else {
            try {
                ContainerLauncher launcher = ContainerLauncher.make(getConf(), getId(), getSharedContext());
                killWorkers(SupervisorUtils.supervisorWorkerIds(conf), launcher);
            } catch (Exception e) {
                throw Utils.wrapInRuntime(e);
            }
        }
    }

    @Override
    public boolean isWaiting() {
        if (!active) {
            return true;
        }

        if (heartbeatTimer.isTimerWaiting() && eventTimer.isTimerWaiting() && eventManager.waiting()) {
            return true;
        }
        return false;
    }

    /**
     * supervisor daemon enter entrance
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
        Utils.setupDefaultUncaughtExceptionHandler();
        @SuppressWarnings("resource")
        Supervisor instance = new Supervisor(new StandaloneSupervisor());
        instance.launchDaemon();
    }
}
