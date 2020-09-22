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
(ns org.apache.storm.ui.helpers
  (:use compojure.core)
  (:use [hiccup core page-helpers])
  (:use [clojure
         [string :only [blank? join]]
         [walk :only [keywordize-keys]]])
  (:use [org.apache.storm config log])
  (:use [org.apache.storm.util :only [clojurify-structure defnk not-nil?]])
  (:use [clj-time coerce format])
  (:import [org.apache.storm.generated ExecutorInfo ExecutorSummary]
           [org.apache.storm.ui UIHelpers]
           [org.apache.storm.metric StormMetricsRegistry])
  (:import [org.apache.storm.logging.filters AccessLoggingFilter])
  (:import [java.util EnumSet]
           [java.net URLEncoder])
  (:require [ring.util servlet]
            [ring.util.response :as response])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]))

;; TODO this function and its callings will be replace when ui.core and logviewer and drpc move to Java
(def num-web-requests (StormMetricsRegistry/registerMeter "num-web-requests"))

(defn requests-middleware
  "Wrap request with Coda Hale metric for counting the number of web requests, 
  and add Cache-Control: no-cache for html files in root directory (index.html, topology.html, etc)"
  [handler]
  (fn [req]
    (.mark num-web-requests)
    (let [uri (:uri req)
          res (handler req) 
          content-type (response/get-header res "Content-Type")]
      ;; check that the response is html and that the path is for a root page: a single / in the path 
      ;; then we know we don't want it cached (e.g. /index.html)
      (if (and (= content-type "text/html") 
               (= 1 (count (re-seq #"/" uri))))
        ;; response for html page in root directory, no-cache 
        (response/header res "Cache-Control" "no-cache")
        ;; else, carry on
        res))))

;; TODO this function and its callings will be replace when ui.core and logviewer move to Java
(defnk json-response
  [data callback :need-serialize true :status 200 :headers {}]
  {:status status
   :headers (UIHelpers/getJsonResponseHeaders callback headers)
   :body (UIHelpers/getJsonResponseBody data callback need-serialize)})
