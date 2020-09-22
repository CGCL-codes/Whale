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

package org.apache.storm.daemon.logviewer.utils;

import static org.apache.storm.Config.NIMBUS_ADMINS;
import static org.apache.storm.Config.TOPOLOGY_GROUPS;
import static org.apache.storm.Config.TOPOLOGY_USERS;
import static org.apache.storm.DaemonConfig.LOGS_USERS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.daemon.logviewer.testsupport.ArgumentsVerifier;
import org.apache.storm.utils.Utils;
import org.junit.Test;

public class ResourceAuthorizerTest {

    /**
     * allow cluster admin.
     */
    @Test
    public void testAuthorizedLogUserAllowClusterAdmin() {
        Map<String, Object> stormConf = Utils.readStormConfig();

        Map<String, Object> conf = new HashMap<>(stormConf);
        conf.put(NIMBUS_ADMINS, Collections.singletonList("alice"));

        ResourceAuthorizer authorizer = spy(new ResourceAuthorizer(conf));

        doReturn(new ResourceAuthorizer.LogUserGroupWhitelist(Collections.emptySet(), Collections.emptySet()))
                .when(authorizer).getLogUserGroupWhitelist(anyString());

        doReturn(Collections.emptySet()).when(authorizer).getUserGroups(anyString());

        assertTrue(authorizer.isAuthorizedLogUser("alice", "non-blank-fname"));

        verifyStubMethodsAreCalledProperly(authorizer);
    }

    /**
     * ignore any cluster-set topology.users topology.groups.
     */
    @Test
    public void testAuthorizedLogUserIgnoreAnyClusterSetTopologyUsersAndTopologyGroups() {
        Map<String, Object> stormConf = Utils.readStormConfig();

        Map<String, Object> conf = new HashMap<>(stormConf);
        conf.put(TOPOLOGY_USERS, Collections.singletonList("alice"));
        conf.put(TOPOLOGY_GROUPS, Collections.singletonList("alice-group"));

        ResourceAuthorizer authorizer = spy(new ResourceAuthorizer(conf));

        doReturn(new ResourceAuthorizer.LogUserGroupWhitelist(Collections.emptySet(), Collections.emptySet()))
                .when(authorizer).getLogUserGroupWhitelist(anyString());

        doReturn(Collections.singleton("alice-group")).when(authorizer).getUserGroups(anyString());

        assertFalse(authorizer.isAuthorizedLogUser("alice", "non-blank-fname"));

        verifyStubMethodsAreCalledProperly(authorizer);
    }

    /**
     * allow cluster logs user.
     */
    @Test
    public void testAuthorizedLogUserAllowClusterLogsUser() {
        Map<String, Object> stormConf = Utils.readStormConfig();

        Map<String, Object> conf = new HashMap<>(stormConf);
        conf.put(LOGS_USERS, Collections.singletonList("alice"));

        ResourceAuthorizer authorizer = spy(new ResourceAuthorizer(conf));

        doReturn(new ResourceAuthorizer.LogUserGroupWhitelist(Collections.emptySet(), Collections.emptySet()))
                .when(authorizer).getLogUserGroupWhitelist(anyString());

        doReturn(Collections.emptySet()).when(authorizer).getUserGroups(anyString());

        assertTrue(authorizer.isAuthorizedLogUser("alice", "non-blank-fname"));

        verifyStubMethodsAreCalledProperly(authorizer);
    }

    /**
     * allow whitelisted topology user.
     */
    @Test
    public void testAuthorizedLogUserAllowWhitelistedTopologyUser() {
        Map<String, Object> stormConf = Utils.readStormConfig();

        Map<String, Object> conf = new HashMap<>(stormConf);

        ResourceAuthorizer authorizer = spy(new ResourceAuthorizer(conf));

        doReturn(new ResourceAuthorizer.LogUserGroupWhitelist(Collections.singleton("alice"), Collections.emptySet()))
                .when(authorizer).getLogUserGroupWhitelist(anyString());

        doReturn(Collections.emptySet()).when(authorizer).getUserGroups(anyString());

        assertTrue(authorizer.isAuthorizedLogUser("alice", "non-blank-fname"));

        verifyStubMethodsAreCalledProperly(authorizer);
    }

    /**
     * allow whitelisted topology group.
     */
    @Test
    public void testAuthorizedLogUserAllowWhitelistedTopologyGroup() {
        Map<String, Object> stormConf = Utils.readStormConfig();

        Map<String, Object> conf = new HashMap<>(stormConf);

        ResourceAuthorizer authorizer = spy(new ResourceAuthorizer(conf));

        doReturn(new ResourceAuthorizer.LogUserGroupWhitelist(Collections.emptySet(), Collections.singleton("alice-group")))
                .when(authorizer).getLogUserGroupWhitelist(anyString());

        doReturn(Collections.singleton("alice-group")).when(authorizer).getUserGroups(anyString());

        assertTrue(authorizer.isAuthorizedLogUser("alice", "non-blank-fname"));

        verifyStubMethodsAreCalledProperly(authorizer);
    }

    /**
     * disallow user not in nimbus admin, topo user, logs user, or whitelist.
     */
    @Test
    public void testAuthorizedLogUserDisallowUserNotInNimbusAdminNorTopoUserNorLogsUserNotWhitelist() {
        Map<String, Object> stormConf = Utils.readStormConfig();

        Map<String, Object> conf = new HashMap<>(stormConf);

        ResourceAuthorizer authorizer = spy(new ResourceAuthorizer(conf));

        doReturn(new ResourceAuthorizer.LogUserGroupWhitelist(Collections.emptySet(), Collections.emptySet()))
                .when(authorizer).getLogUserGroupWhitelist(anyString());

        doReturn(Collections.emptySet()).when(authorizer).getUserGroups(anyString());

        assertFalse(authorizer.isAuthorizedLogUser("alice", "non-blank-fname"));

        verifyStubMethodsAreCalledProperly(authorizer);
    }

    private void verifyStubMethodsAreCalledProperly(ResourceAuthorizer authorizer) {
        ArgumentsVerifier.verifyFirstCallArgsForSingleArgMethod(
            captor -> verify(authorizer, times(2)).getLogUserGroupWhitelist(captor.capture()),
            String.class, "non-blank-fname");

        ArgumentsVerifier.verifyFirstCallArgsForSingleArgMethod(
            captor -> verify(authorizer).getUserGroups(captor.capture()),
            String.class, "alice");
    }
}
