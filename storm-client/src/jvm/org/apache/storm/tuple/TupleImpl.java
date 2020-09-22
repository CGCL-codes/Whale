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
package org.apache.storm.tuple;

import java.util.Collections;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.GeneralTopologyContext;

public class TupleImpl implements Tuple {
    private final List<Object> values;
    private final int taskId;
    private final String streamId;
    private final GeneralTopologyContext context;
    private final MessageId id;
    private Long _processSampleStartTime;
    private Long _executeSampleStartTime;
    private long _outAckVal = 0;
    
    public TupleImpl(Tuple t) {
        this.values = t.getValues();
        this.taskId = t.getSourceTask();
        this.streamId = t.getSourceStreamId();
        this.id = t.getMessageId();
        this.context = t.getContext();
        if (t instanceof TupleImpl) {
            TupleImpl ti = (TupleImpl) t;
            this._processSampleStartTime = ti._processSampleStartTime;
            this._executeSampleStartTime = ti._executeSampleStartTime;
            this._outAckVal = ti._outAckVal;
        }
    }

    public TupleImpl(GeneralTopologyContext context, List<Object> values, int taskId, String streamId, MessageId id) {
        this.values = Collections.unmodifiableList(values);
        this.taskId = taskId;
        this.streamId = streamId;
        this.id = id;
        this.context = context;
        
        String componentId = context.getComponentId(taskId);
        Fields schema = context.getComponentOutputFields(componentId, streamId);
        if(values.size()!=schema.size()) {
            throw new IllegalArgumentException(
                    "Tuple created with wrong number of fields. " +
                    "Expected " + schema.size() + " fields but got " +
                    values.size() + " fields");
        }
    }

    public TupleImpl(GeneralTopologyContext context, List<Object> values, int taskId, String streamId) {
        this(context, values, taskId, streamId, MessageId.makeUnanchored());
    }
    
    public void setProcessSampleStartTime(long ms) {
        _processSampleStartTime = ms;
    }

    public Long getProcessSampleStartTime() {
        return _processSampleStartTime;
    }
    
    public void setExecuteSampleStartTime(long ms) {
        _executeSampleStartTime = ms;
    }

    public Long getExecuteSampleStartTime() {
        return _executeSampleStartTime;
    }
    
    public void updateAckVal(long val) {
        _outAckVal = _outAckVal ^ val;
    }
    
    public long getAckVal() {
        return _outAckVal;
    }
    
    /** Tuple APIs*/
    @Override
    public int size() {
        return values.size();
    }

    @Override
    public int fieldIndex(String field) {
        return getFields().fieldIndex(field);
    }
    
    @Override
    public boolean contains(String field) {
        return getFields().contains(field);
    }
    
    @Override
    public Object getValue(int i) {
        return values.get(i);
    }

    @Override
    public String getString(int i) {
        return (String) values.get(i);
    }

    @Override
    public Integer getInteger(int i) {
        return (Integer) values.get(i);
    }

    @Override
    public Long getLong(int i) {
        return (Long) values.get(i);
    }

    @Override
    public Boolean getBoolean(int i) {
        return (Boolean) values.get(i);
    }

    @Override
    public Short getShort(int i) {
        return (Short) values.get(i);
    }

    @Override
    public Byte getByte(int i) {
        return (Byte) values.get(i);
    }

    @Override
    public Double getDouble(int i) {
        return (Double) values.get(i);
    }

    @Override
    public Float getFloat(int i) {
        return (Float) values.get(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return (byte[]) values.get(i);
    }

    @Override
    public Object getValueByField(String field) {
        return values.get(fieldIndex(field));
    }

    @Override
    public String getStringByField(String field) {
        return (String) values.get(fieldIndex(field));
    }

    @Override
    public Integer getIntegerByField(String field) {
        return (Integer) values.get(fieldIndex(field));
    }

    @Override
    public Long getLongByField(String field) {
        return (Long) values.get(fieldIndex(field));
    }

    @Override
    public Boolean getBooleanByField(String field) {
        return (Boolean) values.get(fieldIndex(field));
    }

    @Override
    public Short getShortByField(String field) {
        return (Short) values.get(fieldIndex(field));
    }

    @Override
    public Byte getByteByField(String field) {
        return (Byte) values.get(fieldIndex(field));
    }

    @Override
    public Double getDoubleByField(String field) {
        return (Double) values.get(fieldIndex(field));
    }

    @Override
    public Float getFloatByField(String field) {
        return (Float) values.get(fieldIndex(field));
    }

    @Override
    public byte[] getBinaryByField(String field) {
        return (byte[]) values.get(fieldIndex(field));
    }

    @Override
    public List<Object> getValues() {
        return values;
    }

    @Override    
    public Fields getFields() {
        return context.getComponentOutputFields(getSourceComponent(), getSourceStreamId());
    }

    @Override
    public List<Object> select(Fields selector) {
        return getFields().select(selector, values);
    }
    
    @Override
    public GlobalStreamId getSourceGlobalStreamid() {
        return getSourceGlobalStreamId();
    }

    @Override
    public GlobalStreamId getSourceGlobalStreamId() {
        return new GlobalStreamId(getSourceComponent(), streamId);
    }

    @Override
    public String getSourceComponent() {
        return context.getComponentId(taskId);
    }

    @Override
    public int getSourceTask() {
        return taskId;
    }

    @Override
    public String getSourceStreamId() {
        return streamId;
    }

    @Override
    public MessageId getMessageId() {
        return id;
    }
    
    @Override
    public GeneralTopologyContext getContext() {
        return context;
    }
    
    @Override
    public String toString() {
        return "source: " + getSourceComponent() + ":" + taskId + ", stream: " + streamId + ", id: "+ id.toString() + ", " + values.toString() + " PROC_START_TIME(sampled): " + _processSampleStartTime + " EXEC_START_TIME(sampled): " + _executeSampleStartTime;
    }
    
    @Override
    public boolean equals(Object other) {
        return this == other;
    }    
    
    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }
}
