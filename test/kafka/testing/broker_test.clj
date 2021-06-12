(ns kafka.testing.broker-test
  (:require
   [clojure.test :refer :all]

   [jackdaw.admin :as ka]

   [kafka.testing.utils :as tu]
   [kafka.testing.zookeeper :as tzk]
   [kafka.testing.broker :as tkb]
   [kafka.testing.test-utils :as ttu])
  (:import [org.apache.kafka.common.errors TimeoutException]))

(def zookeeper-atom (atom nil))

(defn try-connect [bootstrap-servers]
  (try
    (with-open [client (ka/->AdminClient
                         {"bootstrap.servers"      bootstrap-servers
                          "request.timeout.ms"     "200"
                          "default.api.timeout.ms" "200"})]
      (ka/describe-cluster client))
    (catch Exception e (.getCause e))))

(use-fixtures :each
  (tzk/with-fresh-zookeeper zookeeper-atom)
  (tzk/with-running-zookeeper zookeeper-atom))

(deftest kafka-broker-uses-a-random-port
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper-connect-string (tzk/connect-string zookeeper))]
    (is (not (= 9092 (tkb/port broker))))))

(deftest kafka-broker-uses-the-specified-port
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :port 14326
                 :zookeeper-connect-string (tzk/connect-string zookeeper))]
    (is (= 14326 (tkb/port broker)))))

(deftest kafka-broker-uses-localhost-as-hostname
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper-connect-string (tzk/connect-string zookeeper))]
    (is (= "localhost" (tkb/hostname broker)))))

(deftest kafka-broker-uses-the-specified-hostname
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :hostname "kafka.local"
                 :zookeeper-connect-string (tzk/connect-string zookeeper))]
    (is (= "kafka.local" (tkb/hostname broker)))))

(deftest kafka-broker-uses-a-temporary-log-directory
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper-connect-string (tzk/connect-string zookeeper))]
    (is (not (= "/tmp/kafka-logs" (tkb/log-directory broker))))))

(deftest kafka-broker-uses-the-specified-log-directory
  (let [zookeeper (deref zookeeper-atom)
        log-directory (tu/temporary-directory!)
        broker (tkb/kafka-broker
                 :log-directory log-directory
                 :zookeeper-connect-string (tzk/connect-string zookeeper))]
    (is (= log-directory (tkb/log-directory broker)))))

(deftest kafka-broker-does-not-start-the-broker
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper-connect-string (tzk/connect-string zookeeper))
        bootstrap-servers (tkb/bootstrap-servers broker)
        connect-result (try-connect bootstrap-servers)]
    (is (instance? TimeoutException connect-result))))

(deftest start-starts-the-provided-kafka-broker
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper-connect-string (tzk/connect-string zookeeper))
        broker (tkb/start broker)
        bootstrap-servers (tkb/bootstrap-servers broker)
        connect-result (try-connect bootstrap-servers)]
    (is (not (instance? TimeoutException connect-result)))))

(deftest stop-stops-the-provided-kafka-broker
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper-connect-string (tzk/connect-string zookeeper))
        broker (tkb/start broker)
        broker (tkb/stop broker)
        bootstrap-servers (tkb/bootstrap-servers broker)
        log-directory (tkb/log-directory broker)
        connect-result (try-connect bootstrap-servers)]
    (is (false? (ttu/path-exists? log-directory)))
    (is (instance? TimeoutException connect-result))))
