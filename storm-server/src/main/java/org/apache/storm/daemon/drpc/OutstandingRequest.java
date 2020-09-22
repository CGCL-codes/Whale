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
package org.apache.storm.daemon.drpc;

import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.utils.Time;

public abstract class OutstandingRequest {
    private final long _start;
    private final String _function;
    private final DRPCRequest _req;
    private volatile boolean _fetched = false;

    public OutstandingRequest(String function, DRPCRequest req) {
        _start = Time.currentTimeMillis();
        _function = function;
        _req = req;
    }

    public DRPCRequest getRequest() {
        return _req;
    }

    public void fetched() {
        _fetched = true;
    }

    public boolean wasFetched() {
        return _fetched; 
    }

    public String getFunction() {
        return _function;
    }

    public boolean isTimedOut(long timeoutMs) {
        return (_start + timeoutMs) <= Time.currentTimeMillis();
    }

    public abstract void returnResult(String result);
    public abstract void fail(DRPCExecutionException e);
}
