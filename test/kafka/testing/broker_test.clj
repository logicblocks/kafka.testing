(ns kafka.testing.broker-test
  (:require
   [clojure.test :refer :all]

   [kafka.testing.logging]
   [kafka.testing.utils :as tu]
   [kafka.testing.zookeeper :as tzk]
   [kafka.testing.broker :as tkb]
   [kafka.testing.test-utils :as ttu])
  (:import
   [org.apache.kafka.common.errors TimeoutException]))

(def zookeeper-atom (atom nil))

(use-fixtures :each
  (tzk/with-fresh-zookeeper zookeeper-atom)
  (tzk/with-running-zookeeper zookeeper-atom))

(deftest kafka-broker-uses-a-random-port
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper.connect (tzk/connect-string zookeeper))]
    (is (not (= 9092 (tkb/port broker))))))

(deftest kafka-broker-uses-the-specified-port
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :port 14326
                 :zookeeper.connect (tzk/connect-string zookeeper))]
    (is (= 14326 (tkb/port broker)))))

(deftest kafka-broker-uses-localhost-as-host-name
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper.connect (tzk/connect-string zookeeper))]
    (is (= "localhost" (tkb/host-name broker)))))

(deftest kafka-broker-uses-the-specified-host-name
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :host.name "kafka.local"
                 :zookeeper.connect (tzk/connect-string zookeeper))]
    (is (= "kafka.local" (tkb/host-name broker)))))

(deftest kafka-broker-uses-a-temporary-log-directory
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper.connect (tzk/connect-string zookeeper))]
    (is (not (= "/tmp/kafka-logs" (tkb/log-dir broker))))))

(deftest kafka-broker-uses-the-specified-log-directory
  (let [zookeeper (deref zookeeper-atom)
        log-directory (tu/temporary-directory!)
        broker (tkb/kafka-broker
                 :log.dir log-directory
                 :zookeeper.connect (tzk/connect-string zookeeper))]
    (is (= log-directory (tkb/log-dir broker)))))

(deftest kafka-broker-uses-an-offsets-topic-replication-factor-of-1
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper.connect (tzk/connect-string zookeeper))]
    (is (= 1 (tkb/offsets-topic-replication-factor broker)))))

(deftest kafka-broker-uses-a-transaction-topic-replication-factor-of-1
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper.connect (tzk/connect-string zookeeper))]
    (is (= 1 (tkb/transaction-topic-replication-factor broker)))))

(deftest kafka-broker-does-not-start-the-broker
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper.connect (tzk/connect-string zookeeper))
        bootstrap-servers (tkb/bootstrap-servers broker)
        connect-result (ttu/try-connecting-to-kafka-broker bootstrap-servers)]
    (is (instance? TimeoutException connect-result))))

(deftest start-starts-the-provided-kafka-broker
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper.connect (tzk/connect-string zookeeper))
        broker (tkb/start broker)
        bootstrap-servers (tkb/bootstrap-servers broker)
        log-directory (tkb/log-dir broker)
        connect-result (ttu/try-connecting-to-kafka-broker bootstrap-servers)]
    (is (true? (ttu/path-exists? log-directory)))
    (is (not (instance? TimeoutException connect-result)))))

(deftest stop-stops-the-provided-kafka-broker
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper.connect (tzk/connect-string zookeeper))
        broker (tkb/start broker)
        broker (tkb/stop broker)
        bootstrap-servers (tkb/bootstrap-servers broker)
        log-directory (tkb/log-dir broker)
        connect-result (ttu/try-connecting-to-kafka-broker bootstrap-servers)]
    (is (false? (ttu/path-exists? log-directory)))
    (is (instance? TimeoutException connect-result))))

(deftest with-fresh-kafka-broker-instantiates-new-kafka-broker
  (let [broker-atom (atom nil)
        instantiation-fn
        (tkb/with-fresh-kafka-broker broker-atom zookeeper-atom)

        instance-before-invocations (atom nil)
        instance-during-first-invocation (atom nil)
        instance-after-first-invocation (atom nil)
        instance-during-second-invocation (atom nil)
        instance-after-second-invocation (atom nil)]
    (reset! instance-before-invocations @broker-atom)
    (instantiation-fn
      (fn []
        (reset! instance-during-first-invocation @broker-atom)))
    (reset! instance-after-first-invocation @broker-atom)
    (instantiation-fn
      (fn []
        (reset! instance-during-second-invocation @broker-atom)))
    (reset! instance-after-second-invocation @broker-atom)

    (is (nil? @instance-before-invocations))
    (is (not (nil? @instance-during-first-invocation)))
    (is (nil? @instance-after-first-invocation))
    (is (not (nil? @instance-during-second-invocation)))
    (is (nil? @instance-after-second-invocation))
    (is (not (=
               @instance-after-first-invocation
               @instance-during-second-invocation)))))

(deftest with-running-kafka-broker-manages-kafka-broker-lifecycle
  (let [zookeeper (deref zookeeper-atom)
        broker (tkb/kafka-broker
                 :zookeeper.connect (tzk/connect-string zookeeper))
        broker-atom (atom broker)
        lifecycle-fn (tkb/with-running-kafka-broker broker-atom)
        bootstrap-servers (tkb/bootstrap-servers broker)
        log-directory (tkb/log-dir broker)

        connect-result-before
        (ttu/try-connecting-to-kafka-broker bootstrap-servers)
        log-directory-exists-before (ttu/path-exists? log-directory)

        connect-result-during-atom (atom nil)
        log-directory-exists-during-atom (atom nil)
        _ (lifecycle-fn
            (fn []
              (reset! log-directory-exists-during-atom
                (ttu/path-exists? log-directory))
              (reset! connect-result-during-atom
                (ttu/try-connecting-to-kafka-broker bootstrap-servers))))
        connect-result-during (deref connect-result-during-atom)
        log-directory-exists-during (deref log-directory-exists-during-atom)

        connect-result-after
        (ttu/try-connecting-to-kafka-broker bootstrap-servers)
        log-directory-exists-after (ttu/path-exists? log-directory)]
    (is (instance? TimeoutException connect-result-before))
    (is (true? log-directory-exists-before))

    (is (not (instance? TimeoutException connect-result-during)))
    (is (true? log-directory-exists-during))

    (is (instance? TimeoutException connect-result-after))
    (is (false? log-directory-exists-after))))
