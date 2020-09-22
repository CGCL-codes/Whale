;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns integration.org.apache.storm.testing4j-test
  (:use [clojure.test])
  (:use [org.apache.storm daemon-config config util])
  (:use [org.apache.storm clojure])
  (:require [integration.org.apache.storm.integration-test :as it])
  (:require [org.apache.storm.internal.thrift :as thrift])
  (:import [org.apache.storm Testing Config]
           [org.apache.storm.generated GlobalStreamId])
  (:import [org.apache.storm.tuple Values])
  (:import [org.apache.storm.utils Time])
  (:import [org.apache.storm.testing MkClusterParam TestJob MockedSources TestWordSpout FeederSpout
            TestWordCounter TestGlobalCount TestAggregatesCounter CompleteTopologyParam
            AckFailMapTracker MkTupleParam])
  (:import [org.apache.storm.utils Utils])
  (:import [org.apache.storm Thrift ILocalCluster]))

(deftest test-with-simulated-time
  (is (= false (Time/isSimulating)))
  (Testing/withSimulatedTime (fn []
                               (is (= true (Time/isSimulating)))))
  (is (= false (Time/isSimulating))))

(deftest test-with-local-cluster
  (let [mk-cluster-param (doto (MkClusterParam.)
                           (.setSupervisors (int 2))
                           (.setPortsPerSupervisor (int 5)))
        daemon-conf (doto (Config.)
                      (.put SUPERVISOR-ENABLE false)
                      (.put TOPOLOGY-ACKER-EXECUTORS 0))]
    (Testing/withLocalCluster mk-cluster-param (reify TestJob
                                                 (^void run [this ^ILocalCluster cluster]
                                                   (is (not (nil? cluster)))
                                                   (is (not (nil? (.getNimbus cluster)))))))))

(deftest test-with-simulated-time-local-cluster
  (let [mk-cluster-param (doto (MkClusterParam.)
                           (.setSupervisors (int 2)))
        daemon-conf (doto (Config.)
                      (.put SUPERVISOR-ENABLE false)
                      (.put TOPOLOGY-ACKER-EXECUTORS 0))]
    (is (not (Time/isSimulating)))
    (Testing/withSimulatedTimeLocalCluster mk-cluster-param (reify TestJob
                                                              (^void run [this ^ILocalCluster cluster]
                                                                (is (not (nil? cluster)))
                                                                (is (not (nil? (.getNimbus cluster))))
                                                                (is (Time/isSimulating)))))
    (is (not (Time/isSimulating)))))

(deftest test-with-tracked-cluster
  (Testing/withTrackedCluster
   (reify TestJob
     (^void run [this ^ILocalCluster cluster]
       (let [[feeder checker] (it/ack-tracking-feeder ["num"])
             tracked (Testing/mkTrackedTopology
                      cluster
                      (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails feeder)}
                       {"2" (Thrift/prepareBoltDetails
                              {(GlobalStreamId. "1" Utils/DEFAULT_STREAM_ID)
                               (Thrift/prepareShuffleGrouping)}
                              it/identity-bolt)
                        "3" (Thrift/prepareBoltDetails
                              {(GlobalStreamId. "1" Utils/DEFAULT_STREAM_ID)
                               (Thrift/prepareShuffleGrouping)}
                              it/identity-bolt)
                        "4" (Thrift/prepareBoltDetails
                             {(GlobalStreamId. "2" Utils/DEFAULT_STREAM_ID)
                              (Thrift/prepareShuffleGrouping)
                              (GlobalStreamId. "3" Utils/DEFAULT_STREAM_ID)
                              (Thrift/prepareShuffleGrouping)}
                             (it/agg-bolt 4))}))]
         (.submitTopology cluster
                          "test-acking2"
                          (Config.)
                          (.getTopology tracked))
         (.advanceClusterTime cluster (int 11))
         (.feed feeder [1])
         (Testing/trackedWait tracked (int 1))
         (checker 0)
         (.feed feeder [1])
         (Testing/trackedWait tracked (int 1))
         (checker 2)
         )))))

(deftest test-advance-cluster-time
  (let [daemon-conf (doto (Config.)
                      (.put TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS true))
        mk-cluster-param (doto (MkClusterParam.)
                           (.setDaemonConf daemon-conf))]
    (Testing/withSimulatedTimeLocalCluster
     mk-cluster-param
     (reify TestJob
       (^void run [this ^ILocalCluster cluster]
         (let [feeder (FeederSpout. ["field1"])
               tracker (AckFailMapTracker.)
               _ (.setAckFailDelegate feeder tracker)
               topology (Thrift/buildTopology
                         {"1" (Thrift/prepareSpoutDetails feeder)}
                         {"2" (Thrift/prepareBoltDetails
                                {(GlobalStreamId. "1" Utils/DEFAULT_STREAM_ID)
                                 (Thrift/prepareGlobalGrouping)}
                                it/ack-every-other)})
               storm-conf (doto (Config.)
                            (.put TOPOLOGY-MESSAGE-TIMEOUT-SECS 10))]
           (.submitTopology cluster
                            "timeout-tester"
                            storm-conf
                            topology)
           (.feed feeder ["a"] 1)
           (.feed feeder ["b"] 2)
           (.feed feeder ["c"] 3)
           (Testing/advanceClusterTime cluster (int 9))
           (it/assert-acked tracker 1 3)
           (is (not (.isFailed tracker 2)))
           (Testing/advanceClusterTime cluster (int 12))
           (it/assert-failed tracker 2)
           ))))))

(deftest test-disable-tuple-timeout
  (let [daemon-conf (doto (Config.)
                      (.put TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS false))
        mk-cluster-param (doto (MkClusterParam.)
                           (.setDaemonConf daemon-conf))]
    (Testing/withSimulatedTimeLocalCluster
      mk-cluster-param
      (reify TestJob
        (^void run [this ^ILocalCluster cluster]
          (let [feeder (FeederSpout. ["field1"])
                tracker (AckFailMapTracker.)
                _ (.setAckFailDelegate feeder tracker)
                topology (Thrift/buildTopology
                           {"1" (Thrift/prepareSpoutDetails feeder)}
                           {"2" (Thrift/prepareBoltDetails
                                  {(GlobalStreamId. "1" Utils/DEFAULT_STREAM_ID)
                                   (Thrift/prepareGlobalGrouping)}
                                  it/ack-every-other)})
                storm-conf (doto (Config.)
                             (.put TOPOLOGY-MESSAGE-TIMEOUT-SECS 10)
                             (.put TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS false))]
            (.submitTopology cluster
              "disable-timeout-tester"
              storm-conf
              topology)
            (.feed feeder ["a"] 1)
            (.feed feeder ["b"] 2)
            (.feed feeder ["c"] 3)
            (Testing/advanceClusterTime cluster (int 9))
            (it/assert-acked tracker 1 3)
            (is (not (.isFailed tracker 2)))
            (Testing/advanceClusterTime cluster (int 12))
            (is (not (.isFailed tracker 2)))
            ))))))

(deftest test-test-tuple
  (testing "one-param signature"
    (let [tuple (Testing/testTuple ["james" "bond"])]
      (is (= ["james" "bond"] (.getValues tuple)))
      (is (= Utils/DEFAULT_STREAM_ID (.getSourceStreamId tuple)))
      (is (= ["field1" "field2"] (-> tuple .getFields .toList)))
      (is (= "component" (.getSourceComponent tuple)))))
   (testing "two-params signature"
    (let [mk-tuple-param (doto (MkTupleParam.)
                           (.setStream "test-stream")
                           (.setComponent "test-component")
                           (.setFields (into-array String ["fname" "lname"])))
          tuple (Testing/testTuple ["james" "bond"] mk-tuple-param)]
      (is (= ["james" "bond"] (.getValues tuple)))
      (is (= "test-stream" (.getSourceStreamId tuple)))
      (is (= ["fname" "lname"] (-> tuple .getFields .toList)))
      (is (= "test-component" (.getSourceComponent tuple))))))
