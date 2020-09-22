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
package org.apache.storm.scheduler.blacklist;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.DefaultScheduler;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

public class TestBlacklistScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TestBlacklistScheduler.class);

    private static int currentTime = 1468216504;

    @Test
    public void TestBadSupervisor() {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 300);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        topoMap.put(topo1.getId(), topo1);

        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        BlacklistScheduler bs = new BlacklistScheduler(new DefaultScheduler());
        bs.prepare(config);
        bs.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        bs.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        bs.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, supMap, TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        bs.schedule(topologies, cluster);
        Assert.assertEquals("blacklist", Collections.singleton("host-0"), cluster.getBlacklistedHosts());
    }

    @Test
    public void TestBadSlot() {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 300);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        topoMap.put(topo1.getId(), topo1);

        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        BlacklistScheduler bs = new BlacklistScheduler(new DefaultScheduler());
        bs.prepare(config);
        bs.schedule(topologies, cluster);

        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removePortFromSupervisors(supMap, "sup-0", 0), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        bs.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removePortFromSupervisors(supMap, "sup-0", 0), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        bs.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        bs.schedule(topologies, cluster);
        Assert.assertEquals("blacklist", Collections.singleton("host-0"), cluster.getBlacklistedHosts());
    }

    @Test
    public void TestResumeBlacklist() {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 300);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        topoMap.put(topo1.getId(), topo1);

        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        BlacklistScheduler bs = new BlacklistScheduler(new DefaultScheduler());
        bs.prepare(config);
        bs.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        bs.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        bs.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        bs.schedule(topologies, cluster);
        Assert.assertEquals("blacklist", Collections.singleton("host-0"), cluster.getBlacklistedHosts());
        for (int i = 0; i < 300 / 10 - 2; i++) {
            bs.schedule(topologies, cluster);
        }
        Assert.assertEquals("blacklist", Collections.singleton("host-0"), cluster.getBlacklistedHosts());
        bs.schedule(topologies, cluster);
        Assert.assertEquals("blacklist", Collections.emptySet(), cluster.getBlacklistedHosts());
    }

    @Test
    public void TestReleaseBlacklist() {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 300);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        TopologyDetails topo2 = TestUtilsForBlacklistScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, true);
        TopologyDetails topo3 = TestUtilsForBlacklistScheduler.getTopology("topo-3", config, 5, 15, 1, 1, currentTime - 16, true);
        TopologyDetails topo4 = TestUtilsForBlacklistScheduler.getTopology("topo-4", config, 5, 15, 1, 1, currentTime - 32, true);
        topoMap.put(topo1.getId(), topo1);

        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        BlacklistScheduler bs = new BlacklistScheduler(new DefaultScheduler());
        bs.prepare(config);
        bs.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        bs.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        bs.schedule(topologies, cluster);
        cluster = new Cluster(iNimbus, supMap, TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        bs.schedule(topologies, cluster);
        Assert.assertEquals("blacklist", Collections.singleton("host-0"), cluster.getBlacklistedHosts());
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topologies = new Topologies(topoMap);
        cluster = new Cluster(iNimbus, supMap, TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        bs.schedule(topologies, cluster);
        Assert.assertEquals("blacklist", Collections.emptySet(), cluster.getBlacklistedHosts());
    }

    @Test
    public void TestList() {
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME, 200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT, 2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME, 300);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        TopologyDetails topo2 = TestUtilsForBlacklistScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 2, true);
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        Topologies topologies = new Topologies(topoMap);
        BlacklistScheduler bs = new BlacklistScheduler(new DefaultScheduler());
        bs.prepare(config);

        List<Map<Integer, List<Integer>>> faultList = new ArrayList<>();

        faultList.add(new HashMap<>());
        faultList.add(ImmutableMap.of(0, ImmutableList.of(0, 1)));
        faultList.add(ImmutableMap.of(0, new ArrayList<>()));
        for (int i = 0; i < 17; i++) {
            faultList.add(new HashMap<>());
        }
        faultList.add(ImmutableMap.of(0, ImmutableList.of(0, 1)));
        faultList.add(ImmutableMap.of(1, ImmutableList.of(1)));
        for (int i = 0; i < 8; i++) {
            faultList.add(new HashMap<>());
        }
        faultList.add(ImmutableMap.of(0, ImmutableList.of(1)));
        faultList.add(ImmutableMap.of(1, ImmutableList.of(1)));
        for (int i = 0; i < 30; i++) {
            faultList.add(new HashMap<>());
        }

        List<Map<String, SupervisorDetails>> supervisorsList = FaultGenerateUtils.getSupervisorsList(3, 4, faultList);
        Cluster cluster = null;
        int count = 0;
        for (Map<String, SupervisorDetails> supervisors : supervisorsList) {
            cluster = FaultGenerateUtils.nextCluster(cluster, supervisors, iNimbus, config, topologies);
            bs.schedule(topologies, cluster);
            if (count == 0) {
                Set<String> hosts = new HashSet<>();
                Assert.assertEquals("blacklist", hosts, cluster.getBlacklistedHosts());
            } else if (count == 2) {
                Set<String> hosts = new HashSet<>();
                Assert.assertEquals("blacklist", hosts, cluster.getBlacklistedHosts());
            } else if (count == 3) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                Assert.assertEquals("blacklist", hosts, cluster.getBlacklistedHosts());
            } else if (count == 30) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                Assert.assertEquals("blacklist", hosts, cluster.getBlacklistedHosts());
            } else if (count == 31) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                hosts.add("host-1");
                Assert.assertEquals("blacklist", hosts, cluster.getBlacklistedHosts());
            } else if (count == 32) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                hosts.add("host-1");
                Assert.assertEquals("blacklist", hosts, cluster.getBlacklistedHosts());
            } else if (count == 60) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                hosts.add("host-1");
                Assert.assertEquals("blacklist", hosts, cluster.getBlacklistedHosts());
            } else if (count == 61) {
                Set<String> hosts = new HashSet<>();
                hosts.add("host-0");
                Assert.assertEquals("blacklist", hosts, cluster.getBlacklistedHosts());
            } else if (count == 62) {
                Set<String> hosts = new HashSet<>();
                Assert.assertEquals("blacklist", hosts, cluster.getBlacklistedHosts());
            }
            count++;
        }

    }

    @Test
    public void removeLongTimeDisappearFromCache(){
        INimbus iNimbus = new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap = TestUtilsForBlacklistScheduler.genSupervisors(3,4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_TIME,200);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_TOLERANCE_COUNT,2);
        config.put(DaemonConfig.BLACKLIST_SCHEDULER_RESUME_TIME,300);
        Map<String, TopologyDetails> topoMap = new HashMap<>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2,true);
        topoMap.put(topo1.getId(), topo1);

        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        BlacklistScheduler bs = new BlacklistScheduler(new DefaultScheduler());
        bs.prepare(config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap,"sup-0"),TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        for (int i = 0 ; i < 20 ; i++){
            bs.schedule(topologies,cluster);
        }
        Set<String> cached = new HashSet<>();
        cached.add("sup-1");
        cached.add("sup-2");
        Assert.assertEquals(cached,bs.cachedSupervisors.keySet());
        cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removePortFromSupervisors(supMap,"sup-0",0),TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), topologies, config);
        for (int i = 0 ;i < 20 ; i++){
            bs.schedule(topologies, cluster);
        }
        Set<Integer> cachedPorts = Sets.newHashSet(1, 2, 3);
        Assert.assertEquals(cachedPorts, bs.cachedSupervisors.get("sup-0"));
    }
}
