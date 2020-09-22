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

(ns org.apache.storm.internal.thrift
  (:import [org.apache.storm.generated Grouping
            Grouping$_Fields StreamInfo])
  (:import [org.apache.storm.utils NimbusClient Utils ConfigUtils])
  (:import [org.apache.storm.security.auth ReqContext])
  (:use [org.apache.storm util config log]))

;; Leaving this definition as core.clj is using them as a nested keyword argument
;; Must remove once core.clj is ported to java
(def grouping-constants
  {Grouping$_Fields/FIELDS :fields
   Grouping$_Fields/SHUFFLE :shuffle
   Grouping$_Fields/ALL :all
   Grouping$_Fields/NONE :none
   Grouping$_Fields/CUSTOM_SERIALIZED :custom-serialized
   Grouping$_Fields/CUSTOM_OBJECT :custom-object
   Grouping$_Fields/DIRECT :direct
   Grouping$_Fields/LOCAL_OR_SHUFFLE :local-or-shuffle})

;; Leaving this method as core.clj is using them as a nested keyword argument
;; Must remove once core.clj is ported to java
(defn grouping-type
  [^Grouping grouping]
  (grouping-constants (.getSetField grouping)))

(defmacro with-configured-nimbus-connection
  [client-sym & body]
  `(let [conf# (clojurify-structure (ConfigUtils/readStormConfig))
         context# (ReqContext/context)
         user# (if (.principal context#) (.getName (.principal context#)))
         nimbusClient# (NimbusClient/getConfiguredClientAs conf# user#)
         ~client-sym (.getClient nimbusClient#)
         conn# (.transport nimbusClient#)
         ]
     (try
       ~@body
     (finally (.close conn#)))))

;; Leaving this definition as core.clj is using them as a nested keyword argument
;; Must remove once core.clj is ported to java
(defn mk-output-spec
  [output-spec]
  (let [output-spec (if (map? output-spec)
                      output-spec
                      {Utils/DEFAULT_STREAM_ID output-spec})]
    (map-val
      (fn [out]
        (if (instance? StreamInfo out)
          out
          (StreamInfo. out false)))
      output-spec)))
