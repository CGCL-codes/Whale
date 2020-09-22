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

package org.apache.storm.security.auth;

import java.util.Map;

/**
 * Provides a way to renew credentials on behalf of a user.
 */
public interface ICredentialsRenewer {

   /**
    * Called when initializing the service.
    * @param conf the storm cluster configuration.
    */ 
   void prepare(Map<String, Object> conf);

    /**
     * Renew any credentials that need to be renewed. (Update the credentials if needed)
     * @param credentials the credentials that may have something to renew.
     * @param topologyConf topology configuration.
     * @param topologyOwnerPrincipal the full principal name of the owner of the topology
     */
    @SuppressWarnings("deprecation")
    default void renew(Map<String, String> credentials, Map<String, Object> topologyConf, String topologyOwnerPrincipal) {
        renew(credentials, topologyConf);
    }

    /**
     * Renew any credentials that need to be renewed. (Update the credentials if needed)
     * @param credentials the credentials that may have something to renew.
     * @param topologyConf topology configuration.
     */
    @Deprecated
    default void renew(Map<String, String>  credentials, Map<String, Object> topologyConf) {
        throw new IllegalStateException("At least one of the renew methods must be overridden");
    }
}
