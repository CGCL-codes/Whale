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

package org.apache.storm.localizer;

import org.apache.storm.generated.LocalAssignment;

/**
 * A Port and a LocalAssignment used to reference count resources.
 */
class PortAndAssignment {
    private final int port;
    private final LocalAssignment assignment;

    public PortAndAssignment(int port, LocalAssignment assignment) {
        this.port = port;
        this.assignment = assignment;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PortAndAssignment)) {
            return false;
        }
        PortAndAssignment pna = (PortAndAssignment) other;
        return pna.port == port && assignment.equals(pna.assignment);
    }

    public String getToplogyId() {
        return assignment.get_topology_id();
    }

    public String getOwner() {
        return assignment.get_owner();
    }

    @Override
    public int hashCode() {
        return (17 * port) + assignment.hashCode();
    }

    @Override
    public String toString() {
        return "{" + assignment.get_topology_id() + " on " + port + "}";
    }

    /**
     * Return the port associated with this.
     */
    public int getPort() {
        return port;
    }

    /**
     * return the assigment for this.
     */
    public LocalAssignment getAssignment() {
        return assignment;
    }
}
