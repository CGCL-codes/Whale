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

import org.apache.storm.Config;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.localizer.LocalResource;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.LocalState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SupervisorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorUtils.class);

    private static final SupervisorUtils INSTANCE = new SupervisorUtils();
    private static SupervisorUtils _instance = INSTANCE;
    public static void setInstance(SupervisorUtils u) {
        _instance = u;
    }
    public static void resetInstance() {
        _instance = INSTANCE;
    }

    public static void rmrAsUser(Map<String, Object> conf, String id, String path) throws IOException {
        String user = ServerUtils.getFileOwner(path);
        String logPreFix = "rmr " + id;
        List<String> commands = new ArrayList<>();
        commands.add("rmr");
        commands.add(path);
        ClientSupervisorUtils.processLauncherAndWait(conf, user, commands, null, logPreFix);
        if (Utils.checkFileExists(path)) {
            throw new RuntimeException(path + " was not deleted.");
        }
    }

    /**
     * Given the blob information returns the value of the uncompress field, handling it either being a string or a boolean value, or if it's not specified then
     * returns false
     * 
     * @param blobInfo
     * @return
     */
    public static boolean shouldUncompressBlob(Map<String, Object> blobInfo) {
        return ObjectReader.getBoolean(blobInfo.get("uncompress"), false);
    }

    /**
     * Given the blob information returns the value of the workerRestart field, handling it either being a string or a boolean value, or
     * if it's not specified then returns false
     *
     * @param blobInfo the info for the blob.
     * @return true if the blob needs a worker restart by way of the callback else false.
     */
    public static boolean blobNeedsWorkerRestart(Map<String, Object> blobInfo) {
        return ObjectReader.getBoolean(blobInfo.get("workerRestart"), false);
    }

    /**
     * Returns a list of LocalResources based on the blobstore-map passed in
     * 
     * @param blobstoreMap
     * @return
     */
    public static List<LocalResource> blobstoreMapToLocalresources(Map<String, Map<String, Object>> blobstoreMap) {
        List<LocalResource> localResourceList = new ArrayList<>();
        if (blobstoreMap != null) {
            for (Map.Entry<String, Map<String, Object>> map : blobstoreMap.entrySet()) {
                Map<String, Object> blobConf = map.getValue();
                LocalResource localResource = new LocalResource(map.getKey(), shouldUncompressBlob(blobConf), blobNeedsWorkerRestart(blobConf));
                localResourceList.add(localResource);
            }
        }
        return localResourceList;
    }

    public static Collection<String> supervisorWorkerIds(Map<String, Object> conf) {
        String workerRoot = ConfigUtils.workerRoot(conf);
        return ConfigUtils.readDirContents(workerRoot);
    }

    /**
     * map from worker id to heartbeat
     *
     * @param conf
     * @return
     * @throws Exception
     */
    public static Map<String, LSWorkerHeartbeat> readWorkerHeartbeats(Map<String, Object> conf) throws Exception {
        return _instance.readWorkerHeartbeatsImpl(conf);
    }

    public Map<String, LSWorkerHeartbeat> readWorkerHeartbeatsImpl(Map<String, Object> conf) throws Exception {
        Map<String, LSWorkerHeartbeat> workerHeartbeats = new HashMap<>();

        Collection<String> workerIds = SupervisorUtils.supervisorWorkerIds(conf);

        for (String workerId : workerIds) {
            LSWorkerHeartbeat whb = readWorkerHeartbeat(conf, workerId);
            // ATTENTION: whb can be null
            workerHeartbeats.put(workerId, whb);
        }
        return workerHeartbeats;
    }


    /**
     * get worker heartbeat by workerId
     *
     * @param conf
     * @param workerId
     * @return
     * @throws IOException
     */
    private static LSWorkerHeartbeat readWorkerHeartbeat(Map<String, Object> conf, String workerId) {
        return _instance.readWorkerHeartbeatImpl(conf, workerId);
    }

    protected LSWorkerHeartbeat readWorkerHeartbeatImpl(Map<String, Object> conf, String workerId) {
        try {
            LocalState localState = ConfigUtils.workerState(conf, workerId);
            return localState.getWorkerHeartBeat();
        } catch (Exception e) {
            LOG.warn("Failed to read local heartbeat for workerId : {},Ignoring exception.", workerId, e);
            return null;
        }
    }

    public static boolean  isWorkerHbTimedOut(int now, LSWorkerHeartbeat whb, Map<String, Object> conf) {
        return _instance.isWorkerHbTimedOutImpl(now, whb, conf);
    }

    private  boolean  isWorkerHbTimedOutImpl(int now, LSWorkerHeartbeat whb, Map<String, Object> conf) {
        return (now - whb.get_time_secs()) > ObjectReader.getInt(conf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS));
    }

    static List<ACL> supervisorZkAcls() {
        final List<ACL> acls = new ArrayList<>();
        acls.add(ZooDefs.Ids.CREATOR_ALL_ACL.get(0));
        acls.add(new ACL((ZooDefs.Perms.READ ^ ZooDefs.Perms.CREATE), ZooDefs.Ids.ANYONE_ID_UNSAFE));
        return acls;
    }
}
