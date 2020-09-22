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
package org.apache.storm.streams.operations;

/**
 * An interface for joining two values to produce a result.
 *
 * @param <V1> the type of the first value
 * @param <V2> the type of the second value
 * @param <R>  the result type
 */
public interface ValueJoiner<V1, V2, R> extends Operation {
    /**
     * Joins two values and produces a result.
     *
     * @param value1 the first value
     * @param value2 the second value
     * @return the result
     */
    R apply(V1 value1, V2 value2);
}
