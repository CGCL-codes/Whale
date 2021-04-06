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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.trident.state;

import org.apache.storm.trident.state.Serializer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.util.List;

public class SerializedStateMapper<T> implements StateMapper<T> {

    private final Fields stateFields;
    private final Serializer<T> serializer;

    public SerializedStateMapper(String fieldName, Serializer<T> serializer) {
        this.stateFields = new Fields(fieldName);
        this.serializer = serializer;
    }

    @Override
    public Fields getStateFields() {
        return stateFields;
    }

    @Override
    public Values toValues(T value) {
        byte[] serialized = serializer.serialize(value);
        return new Values(ByteBuffer.wrap(serialized));
    }

    @Override
    public T fromValues(List<Values> values) {
        if (values.size() == 0) {
            return null;
        }
        else if (values.size() == 1) {
            ByteBuffer bytes = (ByteBuffer) values.get(0).get(0);
            return serializer.deserialize(bytes.array());
        }
        else {
            throw new IllegalArgumentException("Can only convert single values, " + values.size() + " encountered");
        }
    }

    @Override
    public String toString() {
        return String.format("{type: %s, fields: %s, serializer: %s}", this.getClass().getSimpleName(), stateFields, serializer);
    }
}
