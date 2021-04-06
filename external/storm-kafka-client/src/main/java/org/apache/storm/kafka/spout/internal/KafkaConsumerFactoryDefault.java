/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kafka.spout.internal;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

public class KafkaConsumerFactoryDefault<K, V> implements KafkaConsumerFactory<K, V> {

    @Override
    public KafkaConsumer<K, V> createConsumer(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        return new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps());
    }
    
}
