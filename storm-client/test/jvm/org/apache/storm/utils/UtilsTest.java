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

package org.apache.storm.utils;

import org.apache.storm.Config;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class UtilsTest {

    @Test
    public void isZkAuthenticationConfiguredTopologyTest() {
        Assert.assertFalse(
                "Returns null if given null config",
                Utils.isZkAuthenticationConfiguredTopology(null));

        Assert.assertFalse(
                "Returns false if scheme key is missing",
                Utils.isZkAuthenticationConfiguredTopology(emptyMockMap()));

        Assert.assertFalse(
                "Returns false if scheme value is null",
                Utils.isZkAuthenticationConfiguredTopology(topologyMockMap(null)));

        Assert.assertTrue(
                "Returns true if scheme value is string",
                Utils.isZkAuthenticationConfiguredTopology(topologyMockMap("foobar")));
    }

    private Map topologyMockMap(String value) {
        return mockMap(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, value);
    }

    private Map mockMap(String key, String value) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(key, value);
        return map;
    }

    private Map serverMockMap(String value) {
        return mockMap(Config.STORM_ZOOKEEPER_AUTH_SCHEME, value);
    }

    private Map emptyMockMap() {
        return new HashMap<String, Object>();
    }
    
    private void doParseJvmHeapMemByChildOptsTest(String message, String opt, double expected) {
        doParseJvmHeapMemByChildOptsTest(message, Arrays.asList(opt), expected);
    }
    
    private void doParseJvmHeapMemByChildOptsTest(String message, List<String> opts, double expected) {
        Assert.assertEquals(
                message,
                Utils.parseJvmHeapMemByChildOpts(opts, 123.0).doubleValue(), expected, 0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestK() {
        doParseJvmHeapMemByChildOptsTest("Xmx1024k results in 1 MB", "Xmx1024k", 1.0);
        doParseJvmHeapMemByChildOptsTest("Xmx1024K results in 1 MB", "Xmx1024K", 1.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx1024k results in 1 MB", "-Xmx1024k", 1.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestM() {
        doParseJvmHeapMemByChildOptsTest("Xmx100M results in 100 MB", "Xmx100m", 100.0);
        doParseJvmHeapMemByChildOptsTest("Xmx100m results in 100 MB", "Xmx100M", 100.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx100M results in 100 MB", "-Xmx100m", 100.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestG() {
        doParseJvmHeapMemByChildOptsTest("Xmx1g results in 1024 MB", "Xmx1g", 1024.0);
        doParseJvmHeapMemByChildOptsTest("Xmx1G results in 1024 MB", "Xmx1G", 1024.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx1g results in 1024 MB", "-Xmx1g", 1024.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestNoMatch() {
        doParseJvmHeapMemByChildOptsTest("Unmatched unit results in default", "Xmx1t", 123.0);
        doParseJvmHeapMemByChildOptsTest("Unmatched option results in default", "Xms1g", 123.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestNulls() {
        doParseJvmHeapMemByChildOptsTest("Null value results in default", (String) null, 123.0);
        doParseJvmHeapMemByChildOptsTest("Null list results in default", (List<String>) null, 123.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestExtraChars() {
        doParseJvmHeapMemByChildOptsTest("Leading characters are ignored", "---Xmx1g", 1024.0);
        doParseJvmHeapMemByChildOptsTest("Trailing characters are ignored", "Xmx1ggggg", 1024.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestFirstMatch() {
        doParseJvmHeapMemByChildOptsTest("First valid match is used", 
                Arrays.asList(null, "Xmx1t", "Xmx1g", "Xms1024k Xmx1024k", "Xmx100m"), 
                1024.0);
    }

    public void getConfiguredClientThrowsRuntimeExceptionOnBadArgsTest () throws TTransportException {
        Map<String, Object> config = ConfigUtils.readStormConfig();
        config.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);

        try {
            new NimbusClient(config, "", 65535);
            Assert.fail("Expected exception to be thrown");
        } catch (RuntimeException e){
            Assert.assertTrue(
                    "Cause is not TTransportException " + e,
                    Utils.exceptionCauseIsInstanceOf(TTransportException.class, e));
        }
    }

    @Test
    public void isZkAuthenticationConfiguredStormServerTest() {
        Assert.assertFalse(
                "Returns false if given null config",
                Utils.isZkAuthenticationConfiguredStormServer(null));

        Assert.assertFalse(
                "Returns false if scheme key is missing",
                Utils.isZkAuthenticationConfiguredStormServer(emptyMockMap()));

        Assert.assertFalse(
                "Returns false if scheme value is null",
                Utils.isZkAuthenticationConfiguredStormServer(serverMockMap(null)));

        Assert.assertTrue(
                "Returns true if scheme value is string",
                Utils.isZkAuthenticationConfiguredStormServer(serverMockMap("foobar")));
    }

    @Test
    public void isZkAuthenticationConfiguredStormServerWithPropertyTest() {
        String key = "java.security.auth.login.config";
        String oldValue = System.getProperty(key);
        try {
            System.setProperty("java.security.auth.login.config", "anything");
            Assert.assertTrue(Utils.isZkAuthenticationConfiguredStormServer(emptyMockMap()));
        } finally {
            // reset property
            if (oldValue == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, oldValue);
            }
        }
    }
}
