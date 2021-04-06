/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout.trident;

import java.io.Serializable;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.trident.spout.ISpoutPartition;

/**
 * {@link ISpoutPartition} that wraps {@link TopicPartition} information.
 */
public class KafkaTridentSpoutTopicPartition implements ISpoutPartition, Serializable {
    private TopicPartition topicPartition;

    public KafkaTridentSpoutTopicPartition(String topic, int partition) {
        this(new TopicPartition(topic, partition));
    }

    public KafkaTridentSpoutTopicPartition(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    @Override
    public String getId() {
        return topicPartition.topic() + "@" + topicPartition.partition();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KafkaTridentSpoutTopicPartition that = (KafkaTridentSpoutTopicPartition) o;

        return topicPartition != null ? topicPartition.equals(that.topicPartition) : that.topicPartition == null;
    }

    @Override
    public int hashCode() {
        return topicPartition != null ? topicPartition.hashCode() : 0;
    }

    @Override
    public String toString()  {
        return topicPartition.toString();
    }
}
