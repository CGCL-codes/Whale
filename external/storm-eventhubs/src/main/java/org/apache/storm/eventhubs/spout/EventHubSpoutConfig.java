/*******************************************************************************
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
 *******************************************************************************/
package org.apache.storm.eventhubs.spout;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class EventHubSpoutConfig implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final String EH_SERVICE_FQDN_SUFFIX = "servicebus.windows.net";
	private final String userName;
	private final String password;
	private final String namespace;
	private final String entityPath;
	private final int partitionCount;

	private String zkConnectionString = null; // if null then use zookeeper used
												// by Storm
	private int checkpointIntervalInSeconds = 10;
	private int receiverCredits = 1024;
	private int maxPendingMsgsPerPartition = 1024;
	private long enqueueTimeFilter = 0; // timestamp in millisecond, 0 means
										// disabling filter
	private String connectionString;
	private String topologyName;
	private IEventDataScheme scheme = new StringEventDataScheme();
	private String consumerGroupName = EventHubClient.DEFAULT_CONSUMER_GROUP_NAME;
	private String outputStreamId;


	// These are mandatory parameters
	public EventHubSpoutConfig(String username, String password,
			String namespace, String entityPath, int partitionCount) {
		this.userName = username;
		this.password = password;
		this.connectionString = new ConnectionStringBuilder(namespace,entityPath,
				username,password).toString();
		this.namespace = namespace;
		this.entityPath = entityPath;
		this.partitionCount = partitionCount;
	}

	// Keep this constructor for backward compatibility
	public EventHubSpoutConfig(String username, String password,
			String namespace, String entityPath, int partitionCount,
			String zkConnectionString) {
		this(username, password, namespace, entityPath, partitionCount);
		setZkConnectionString(zkConnectionString);
	}

	// Keep this constructor for backward compatibility
	public EventHubSpoutConfig(String username, String password,
			String namespace, String entityPath, int partitionCount,
			String zkConnectionString, int checkpointIntervalInSeconds,
			int receiverCredits) {
		this(username, password, namespace, entityPath, partitionCount,
				zkConnectionString);
		setCheckpointIntervalInSeconds(checkpointIntervalInSeconds);
		setReceiverCredits(receiverCredits);
	}

	public EventHubSpoutConfig(String username, String password,
			String namespace, String entityPath, int partitionCount,
			String zkConnectionString, int checkpointIntervalInSeconds,
			int receiverCredits, long enqueueTimeFilter) {
		this(username, password, namespace, entityPath, partitionCount,
				zkConnectionString, checkpointIntervalInSeconds,
				receiverCredits);
		setEnqueueTimeFilter(enqueueTimeFilter);
	}

	// Keep this constructor for backward compatibility
	public EventHubSpoutConfig(String username, String password,
			String namespace, String entityPath, int partitionCount,
			String zkConnectionString, int checkpointIntervalInSeconds,
			int receiverCredits, int maxPendingMsgsPerPartition,
			long enqueueTimeFilter) {

		this(username, password, namespace, entityPath, partitionCount,
				zkConnectionString, checkpointIntervalInSeconds,
				receiverCredits);
		setMaxPendingMsgsPerPartition(maxPendingMsgsPerPartition);
		setEnqueueTimeFilter(enqueueTimeFilter);
	}

	public String getNamespace() {
		return namespace;
	}

	public String getEntityPath() {
		return entityPath;
	}

	public int getPartitionCount() {
		return partitionCount;
	}

	public String getZkConnectionString() {
		return zkConnectionString;
	}

	public void setZkConnectionString(String value) {
		zkConnectionString = value;
	}

	public EventHubSpoutConfig withZkConnectionString(String value) {
		setZkConnectionString(value);
		return this;
	}

	public int getCheckpointIntervalInSeconds() {
		return checkpointIntervalInSeconds;
	}

	public void setCheckpointIntervalInSeconds(int value) {
		checkpointIntervalInSeconds = value;
	}

	public EventHubSpoutConfig withCheckpointIntervalInSeconds(int value) {
		setCheckpointIntervalInSeconds(value);
		return this;
	}

	public int getReceiverCredits() {
		return receiverCredits;
	}

	public void setReceiverCredits(int value) {
		receiverCredits = value;
	}

	public EventHubSpoutConfig withReceiverCredits(int value) {
		setReceiverCredits(value);
		return this;
	}

	public int getMaxPendingMsgsPerPartition() {
		return maxPendingMsgsPerPartition;
	}

	public void setMaxPendingMsgsPerPartition(int value) {
		maxPendingMsgsPerPartition = value;
	}

	public EventHubSpoutConfig withMaxPendingMsgsPerPartition(int value) {
		setMaxPendingMsgsPerPartition(value);
		return this;
	}

	public long getEnqueueTimeFilter() {
		return enqueueTimeFilter;
	}

	public void setEnqueueTimeFilter(long value) {
		enqueueTimeFilter = value;
	}

	public EventHubSpoutConfig withEnqueueTimeFilter(long value) {
		setEnqueueTimeFilter(value);
		return this;
	}

	public String getTopologyName() {
		return topologyName;
	}

	public void setTopologyName(String value) {
		topologyName = value;
	}

	public EventHubSpoutConfig withTopologyName(String value) {
		setTopologyName(value);
		return this;
	}

	public IEventDataScheme getEventDataScheme() {
		return scheme;
	}

	public void setEventDataScheme(IEventDataScheme scheme) {
		this.scheme = scheme;
	}

	public EventHubSpoutConfig withEventDataScheme(IEventDataScheme value) {
		setEventDataScheme(value);
		return this;
	}

	public String getConsumerGroupName() {
		return consumerGroupName;
	}

	public void setConsumerGroupName(String value) {
		consumerGroupName = value;
	}

	public EventHubSpoutConfig withConsumerGroupName(String value) {
		setConsumerGroupName(value);
		return this;
	}

	public List<String> getPartitionList() {
		List<String> partitionList = new ArrayList<String>();

		for (int i = 0; i < this.partitionCount; i++) {
			partitionList.add(Integer.toString(i));
		}

		return partitionList;
	}

	public String getConnectionString() {
		return connectionString;
	}

	/*Keeping it for backward compatibility*/
	public void setTargetAddress(String targetFqnAddress) {
	}

	public void setTargetAddress(){

	}

	public EventHubSpoutConfig withTargetAddress(String targetFqnAddress) {
		setTargetAddress(targetFqnAddress);
		return this;
	}

	public String getOutputStreamId() {
		return outputStreamId;
	}

	public void setOutputStreamId(String outputStreamId) {
		this.outputStreamId = outputStreamId;
	}
}
