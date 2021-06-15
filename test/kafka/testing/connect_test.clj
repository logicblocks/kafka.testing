(ns kafka.testing.connect-test
  (:require
   [clojure.test :refer :all]

   [kafka.testing.logging]
   [kafka.testing.utils :as tu]
   [kafka.testing.zookeeper :as tzk]
   [kafka.testing.broker :as tkb]
   [kafka.testing.connect :as tkc]
   [kafka.testing.test-utils :as ttu])
  (:import
   [org.sourcelab.kafka.connect.apiclient
    Configuration
    KafkaConnectClient]
   [org.sourcelab.kafka.connect.apiclient.rest.exceptions
    ConnectionException]))

(defn try-connect [rest-url]
  (try
    (let [configuration (Configuration. rest-url)
          client (KafkaConnectClient. configuration)]
      (.getConnectServerVersion client))
    (catch Exception e e)))

(def zookeeper-atom (atom nil))
(def kafka-broker-atom (atom nil))

(use-fixtures :each
  (tzk/with-fresh-zookeeper zookeeper-atom)
  (tzk/with-running-zookeeper zookeeper-atom)
  (tkb/with-fresh-kafka-broker kafka-broker-atom zookeeper-atom)
  (tkb/with-running-kafka-broker kafka-broker-atom))

(deftest kafka-connect-server-uses-a-random-port
  (let [kafka-broker (deref kafka-broker-atom)
        bootstrap-servers (tkb/bootstrap-servers kafka-broker)
        kafka-connect-server (tkc/kafka-connect-server
                               :bootstrap-servers bootstrap-servers)]
    (is (not (= 2181 (tkc/port kafka-connect-server))))))

(deftest kafka-connect-server-uses-the-specified-port
  (let [port (tu/free-port!)
        kafka-broker (deref kafka-broker-atom)
        bootstrap-servers (tkb/bootstrap-servers kafka-broker)
        kafka-connect-server (tkc/kafka-connect-server
                               :port port
                               :bootstrap-servers bootstrap-servers)]
    (is (= port (tkc/port kafka-connect-server)))))

(deftest kafka-connect-server-uses-localhost-as-hostname
  (let [kafka-broker (deref kafka-broker-atom)
        bootstrap-servers (tkb/bootstrap-servers kafka-broker)
        kafka-connect-server (tkc/kafka-connect-server
                               :bootstrap-servers bootstrap-servers)]
    (is (= "localhost" (tkc/hostname kafka-connect-server)))))

(deftest kafka-connect-server-uses-the-specified-hostname
  (let [kafka-broker (deref kafka-broker-atom)
        bootstrap-servers (tkb/bootstrap-servers kafka-broker)
        kafka-connect-server (tkc/kafka-connect-server
                               :hostname "kafka-connect.local"
                               :bootstrap-servers bootstrap-servers)]
    (is (= "kafka-connect.local" (tkc/hostname kafka-connect-server)))))

(deftest kafka-connect-server-uses-a-temporary-offset-storage-file
  (let [kafka-broker (deref kafka-broker-atom)
        bootstrap-servers (tkb/bootstrap-servers kafka-broker)
        kafka-connect-server (tkc/kafka-connect-server
                               :bootstrap-servers bootstrap-servers)]
    (is (not (= "" (tkc/offset-storage-file kafka-connect-server))))))

(deftest kafka-connect-server-uses-the-specified-offset-storage-directory
  (let [kafka-broker (deref kafka-broker-atom)
        bootstrap-servers (tkb/bootstrap-servers kafka-broker)
        offset-storage-file (str (tu/temporary-directory!) "/offset-storage")
        kafka-connect-server (tkc/kafka-connect-server
                               :offset-storage-file offset-storage-file
                               :bootstrap-servers bootstrap-servers)]
    (is (= offset-storage-file
          (tkc/offset-storage-file kafka-connect-server)))))

(deftest kafka-connect-server-does-not-start-the-server
  (let [kafka-broker (deref kafka-broker-atom)
        bootstrap-servers (tkb/bootstrap-servers kafka-broker)
        kafka-connect-server (tkc/kafka-connect-server
                               :bootstrap-servers bootstrap-servers)
        admin-url (tkc/admin-url kafka-connect-server)
        connect-result (try-connect admin-url)]
    (is (instance? ConnectionException connect-result))))

(deftest start-starts-the-provided-kafka-connect-server
  (let [kafka-broker (deref kafka-broker-atom)
        bootstrap-servers (tkb/bootstrap-servers kafka-broker)
        kafka-connect-server (tkc/kafka-connect-server
                               :bootstrap-servers bootstrap-servers)
        kafka-connect-server (tkc/start kafka-connect-server)
        admin-url (tkc/admin-url kafka-connect-server)
        connect-result (try-connect admin-url)]
    (is (not (instance? ConnectionException connect-result)))))

(deftest stop-stops-the-provided-kafka-connect-server
  (let [kafka-broker (deref kafka-broker-atom)
        bootstrap-servers (tkb/bootstrap-servers kafka-broker)
        kafka-connect-server (tkc/kafka-connect-server
                               :bootstrap-servers bootstrap-servers)
        kafka-connect-server (tkc/start kafka-connect-server)
        kafka-connect-server (tkc/stop kafka-connect-server)
        admin-url (tkc/admin-url kafka-connect-server)
        connect-result (try-connect admin-url)]
    (is (instance? ConnectionException connect-result))))

(deftest with-fresh-kafka-connect-server-instantiates-new-kafka-connect-server
  (let [kafka-connect-atom (atom nil)
        instantiation-fn
        (tkc/with-fresh-kafka-connect-server
          kafka-connect-atom kafka-broker-atom)

        instance-before-invocations (atom nil)
        instance-during-first-invocation (atom nil)
        instance-after-first-invocation (atom nil)
        instance-during-second-invocation (atom nil)
        instance-after-second-invocation (atom nil)]
    (reset! instance-before-invocations @kafka-connect-atom)
    (instantiation-fn
      (fn []
        (reset! instance-during-first-invocation @kafka-connect-atom)))
    (reset! instance-after-first-invocation @kafka-connect-atom)
    (instantiation-fn
      (fn []
        (reset! instance-during-second-invocation @kafka-connect-atom)))
    (reset! instance-after-second-invocation @kafka-connect-atom)

    (is (nil? @instance-before-invocations))
    (is (not (nil? @instance-during-first-invocation)))
    (is (nil? @instance-after-first-invocation))
    (is (not (nil? @instance-during-second-invocation)))
    (is (nil? @instance-after-second-invocation))
    (is (not (= @instance-after-first-invocation
               @instance-during-second-invocation)))))

; TODO: Test offset file deletion. Currently, the offset file isn't created
;       until needed so hard to test.
(deftest with-running-kafka-connect-server-manages-kafka-connect-server-lifecycle
  (let [broker (deref kafka-broker-atom)
        connect-server (tkc/kafka-connect-server
                         :bootstrap-servers (tkb/bootstrap-servers broker))
        connect-server-atom (atom connect-server)

        lifecycle-fn (tkc/with-running-kafka-connect-server
                       connect-server-atom)

        admin-url (tkc/admin-url connect-server)

        connect-result-before (try-connect admin-url)

        connect-result-during-atom (atom nil)
        _ (lifecycle-fn
            (fn []
              (reset! connect-result-during-atom
                (try-connect admin-url))))
        connect-result-during (deref connect-result-during-atom)

        connect-result-after (try-connect admin-url)]
    (is (instance? ConnectionException connect-result-before))

    (is (not (instance? ConnectionException connect-result-during)))

    (is (instance? ConnectionException connect-result-after))))
