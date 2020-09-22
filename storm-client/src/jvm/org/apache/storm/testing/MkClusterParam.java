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
package org.apache.storm.testing;

import java.util.Map;

/**
 * The param arg for `Testing.withSimulatedTimeCluster`, `Testing.withTrackedCluster` and `Testing.withLocalCluster` 
 */
public class MkClusterParam {
	/**
	 * count of supervisors for the cluster.
	 */
	private Integer supervisors;
	/**
	 * count of port for each supervisor
	 */
	private Integer portsPerSupervisor;
	/**
	 * cluster config
	 */
	private Map<String, Object> daemonConf;

	private Boolean nimbusDaemon;
	
	public Integer getSupervisors() {
		return supervisors;
	}
	public void setSupervisors(Integer supervisors) {
		this.supervisors = supervisors;
	}
	public Integer getPortsPerSupervisor() {
		return portsPerSupervisor;
	}
	public Boolean isNimbusDaemon() {
	  return nimbusDaemon;
	}
	public void setPortsPerSupervisor(Integer portsPerSupervisor) {
		this.portsPerSupervisor = portsPerSupervisor;
	}
	public Map<String, Object> getDaemonConf() {
		return daemonConf;
	}
	public void setDaemonConf(Map<String, Object> daemonConf) {
		this.daemonConf = daemonConf;
	}
	/**
	* When nimbusDaemon is true, the local cluster will be started with a Nimbus
	* Thrift server, allowing communication through for example org.apache.storm.utils.NimbusClient
	*/
	public void setNimbusDaemon(Boolean nimbusDaemon) {
	  this.nimbusDaemon = nimbusDaemon;
	}
}
