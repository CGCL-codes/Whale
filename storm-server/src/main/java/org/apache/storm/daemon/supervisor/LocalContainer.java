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
package org.apache.storm.daemon.supervisor;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.ProcessSimulator;
import org.apache.storm.daemon.worker.Worker;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.messaging.IContext;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalContainer extends Container {
    private static final Logger LOG = LoggerFactory.getLogger(LocalContainer.class);
    private volatile boolean _isAlive = false;
    private final IContext _sharedContext;
    
    public LocalContainer(Map<String, Object> conf, String supervisorId, int port, LocalAssignment assignment, IContext sharedContext) throws IOException {
        super(ContainerType.LAUNCH, conf, supervisorId, port, assignment, null, null, null, null);
        _sharedContext = sharedContext;
        _workerId = Utils.uuid();
    }

    @Override
    protected void createArtifactsLink() {
        //NOOP no need to create links in local mode
    }

    @Override
    protected void createBlobstoreLinks() {
        // NOOP no need to create links in local mode
    }

    @Override
    public void launch() throws IOException {
        Worker worker = new Worker(_conf, _sharedContext, _topologyId, _supervisorId, _port, _workerId);
        try {
            worker.start();
        } catch (Exception e) {
            throw new IOException(e);
        }
        saveWorkerUser(System.getProperty("user.name"));
        ProcessSimulator.registerProcess(_workerId, worker);
        _isAlive = true;
    }

    @Override
    public void kill() throws IOException {
        ProcessSimulator.killProcess(_workerId);
        _isAlive = false;
        //Make sure the worker is down before we try to shoot any child processes
        super.kill();
    }

    @Override
    public boolean areAllProcessesDead() throws IOException {
        return !_isAlive && super.areAllProcessesDead();
    }

    @Override
    public void relaunch() throws IOException {
        LOG.warn("NOOP relaunch in local mode...");
    }

    @Override
    public boolean didMainProcessExit() {
        //In local mode the main process should never exit on it's own
        return false;
    }

    @Override
    public boolean runProfiling(ProfileRequest request, boolean stop) throws IOException, InterruptedException {
        throw new RuntimeException("Profiling requests are not supported in local mode");
    }
}
