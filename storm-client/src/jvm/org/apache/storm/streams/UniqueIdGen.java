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
package org.apache.storm.streams;

class UniqueIdGen {
    private int streamCounter = 0;
    private int spoutCounter = 0;
    private int boltCounter = 0;
    private static final UniqueIdGen instance = new UniqueIdGen();

    private UniqueIdGen() {
    }

    static UniqueIdGen getInstance() {
        return instance;
    }

    String getUniqueStreamId() {
        streamCounter++;
        return "s" + streamCounter;
    }

    String getUniqueBoltId() {
        boltCounter++;
        return "bolt" + boltCounter;
    }

    String getUniqueSpoutId() {
        spoutCounter++;
        return "spout" + spoutCounter;
    }

    // for unit tests
    void reset() {
        streamCounter = 0;
        spoutCounter = 0;
        boltCounter = 0;
    }

}

