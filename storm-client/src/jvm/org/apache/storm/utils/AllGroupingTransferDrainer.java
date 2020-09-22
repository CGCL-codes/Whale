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
package org.apache.storm.utils;

import org.apache.storm.generated.NodeInfo;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.WorkerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 优化AllGrouping
 * 发送TaskMessages到远程RemoteWorker的工具类
 */
public class AllGroupingTransferDrainer {

  private Map<NodeInfo, ArrayList<WorkerMessage>> bundles = new HashMap();
  private static final Logger LOG = LoggerFactory.getLogger(AllGroupingTransferDrainer.class);
  
  public void add(HashMap<NodeInfo, WorkerMessage> taskTupleSetMap) {
    for (Map.Entry<NodeInfo, WorkerMessage> entry : taskTupleSetMap.entrySet()) {
      addListRefToMap(this.bundles, entry.getKey(), entry.getValue());
    }
  }
  
  public void send(Map<NodeInfo, IConnection> connections) {

    for (Map.Entry<NodeInfo, ArrayList<WorkerMessage>> entry : bundles.entrySet()) {
      NodeInfo hostPort = entry.getKey();
      IConnection connection = connections.get(hostPort);
      if (null != connection) {
        List<WorkerMessage> bundle = entry.getValue();
        for(WorkerMessage workerMessage :bundle){
          LOG.debug("NodeInfo : {} WorkerMessage: {}",hostPort,workerMessage);
          connection.send(workerMessage);
        }
      } else {
        LOG.warn("Connection is not available for hostPort {}", hostPort);
      }
    }
  }

  private <T> void addListRefToMap(Map<T, ArrayList<WorkerMessage>> bundleMap,
                                   T key, WorkerMessage tuples) {
    ArrayList<WorkerMessage> bundle = bundleMap.get(key);

    if (null == bundle) {
      bundle = new ArrayList<WorkerMessage>();
      bundleMap.put(key, bundle);
    }

    if (null != tuples && tuples.tasks().size() > 0) {
      bundle.add(tuples);
    }
  }

  public void clear() {
    bundles.clear();
  }
}
