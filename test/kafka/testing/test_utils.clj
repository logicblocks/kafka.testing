(ns kafka.testing.test-utils
  (:require
   [clojure.java.io :as io]

   [jackdaw.admin :as ka]

   [zookeeper :as zk])
  (:import
   [org.sourcelab.kafka.connect.apiclient
    Configuration
    KafkaConnectClient]))

(defn path-exists? [path]
  (.exists (io/file path)))

(defn try-connecting-to-zookeeper [connect-string]
  (try
    (zk/connect connect-string :timeout-msec 250)
    (catch IllegalStateException e e)))

(defn try-connecting-to-kafka-broker [bootstrap-servers]
  (try
    (with-open [client
                (ka/->AdminClient
                  {"bootstrap.servers"      bootstrap-servers
                   "request.timeout.ms"     "250"
                   "default.api.timeout.ms" "250"})]
      (ka/describe-cluster client))
    (catch Exception e (.getCause e))))

(defn try-connecting-to-kafka-connect [rest-url]
  (try
    (let [configuration (Configuration. rest-url)
          client (KafkaConnectClient. configuration)]
      (.getConnectServerVersion client))
    (catch Exception e e)))
