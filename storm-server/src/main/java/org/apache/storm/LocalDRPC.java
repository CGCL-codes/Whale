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
package org.apache.storm;

import java.util.Map;

import org.apache.storm.daemon.drpc.DRPC;
import org.apache.storm.daemon.drpc.DRPCThrift;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.utils.ServiceRegistry;
import org.apache.storm.utils.Utils;
import org.apache.thrift.TException;

/**
 * A Local way to test DRPC
 * 
 * try (LocalDRPC drpc = new LocalDRPC()) {
 *   // Do tests
 * }
 */
public class LocalDRPC implements ILocalDRPC {

    private final DRPC drpc;
    private final String serviceId;

    public LocalDRPC() {
        Map<String, Object> conf = Utils.readStormConfig();
        drpc = new DRPC(conf);
        serviceId = ServiceRegistry.registerService(new DRPCThrift(drpc));
    }

    @Override
    public String getServiceId() {
        return serviceId;
    }

    @Override
    public void result(String id, String result) throws AuthorizationException, TException {
        drpc.returnResult(id, result);
    }

    @Override
    public String execute(String functionName, String funcArgs) throws DRPCExecutionException, AuthorizationException, TException {
        return drpc.executeBlocking(functionName, funcArgs);
    }

    @Override
    public void failRequest(String id) throws AuthorizationException, TException {
        drpc.failRequest(id, null);
    }
    

    @Override
    public void failRequestV2(String id, DRPCExecutionException e) throws AuthorizationException, TException {
        drpc.failRequest(id, e);
    }

    @Override
    public DRPCRequest fetchRequest(String functionName) throws AuthorizationException, TException {
        return drpc.fetchRequest(functionName);
    }

    @Override
    public void close() {
        ServiceRegistry.unregisterService(this.serviceId);
        drpc.close();
    }
    
    @Override
    public void shutdown() {
        close();
    }
}
