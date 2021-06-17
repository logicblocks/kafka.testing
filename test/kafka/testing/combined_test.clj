(ns kafka.testing.combined-test
  (:require
   [clojure.test :refer :all]

   [kafka.testing.logging]
   [kafka.testing.combined :as tc]
   [kafka.testing.test-utils :as ttu]
   [kafka.testing.utils :as tu]
   [kafka.testing.zookeeper :as tzk]
   [kafka.testing.broker :as tkb]
   [kafka.testing.connect :as tkc]))

(deftest with-kafka-runs-all-components-on-random-ports-by-default
  (let [kafka-atom (atom nil)
        instantiation-fn (tc/with-kafka kafka-atom)
        called (atom false)]
    (instantiation-fn
      (fn []
        (let [kafka (deref kafka-atom)
              zookeeper-connect-result
              (ttu/try-connecting-to-zookeeper
                (tc/zookeeper-connect-string kafka))
              kafka-broker-connect-result
              (ttu/try-connecting-to-kafka-broker
                (tc/kafka-bootstrap-servers kafka))
              kafka-connect-connect-result
              (ttu/try-connecting-to-kafka-connect
                (tc/kafka-connect-admin-url kafka))]
          (reset! called true)
          (is (not (instance? Exception zookeeper-connect-result)))
          (is (not (instance? Exception kafka-broker-connect-result)))
          (is (not (instance? Exception kafka-connect-connect-result))))))
    (is (true? @called))))

(deftest with-kafka-uses-provided-zookeeper-configuration
  (let [kafka-atom (atom nil)
        zookeeper-port (tu/free-port!)
        instantiation-fn (tc/with-kafka kafka-atom
                           :zookeeper {:port zookeeper-port})
        called (atom false)]
    (instantiation-fn
      (fn []
        (let [kafka (deref kafka-atom)
              zookeeper-connect-result
              (ttu/try-connecting-to-zookeeper
                (tc/zookeeper-connect-string kafka))
              kafka-broker-connect-result
              (ttu/try-connecting-to-kafka-broker
                (tc/kafka-bootstrap-servers kafka))
              kafka-connect-connect-result
              (ttu/try-connecting-to-kafka-connect
                (tc/kafka-connect-admin-url kafka))]
          (reset! called true)
          (is (= zookeeper-port (tzk/port (tc/zookeeper kafka))))
          (is (not (instance? Exception zookeeper-connect-result)))
          (is (not (instance? Exception kafka-broker-connect-result)))
          (is (not (instance? Exception kafka-connect-connect-result))))))
    (is (true? @called))))

(deftest with-kafka-uses-provided-kafka-configuration
  (let [kafka-atom (atom nil)
        kafka-port (tu/free-port!)
        instantiation-fn (tc/with-kafka kafka-atom
                           :kafka {:port kafka-port})
        called (atom false)]
    (instantiation-fn
      (fn []
        (let [kafka (deref kafka-atom)
              zookeeper-connect-result
              (ttu/try-connecting-to-zookeeper
                (tc/zookeeper-connect-string kafka))
              kafka-broker-connect-result
              (ttu/try-connecting-to-kafka-broker
                (tc/kafka-bootstrap-servers kafka))
              kafka-connect-connect-result
              (ttu/try-connecting-to-kafka-connect
                (tc/kafka-connect-admin-url kafka))]
          (reset! called true)
          (is (= kafka-port (tkb/port (tc/kafka kafka))))
          (is (not (instance? Exception zookeeper-connect-result)))
          (is (not (instance? Exception kafka-broker-connect-result)))
          (is (not (instance? Exception kafka-connect-connect-result))))))
    (is (true? @called))))

(deftest with-kafka-uses-provided-kafka-connect-configuration
  (let [kafka-atom (atom nil)
        kafka-connect-port (tu/free-port!)
        instantiation-fn (tc/with-kafka kafka-atom
                           :kafka-connect {:rest.port kafka-connect-port})
        called (atom false)]
    (instantiation-fn
      (fn []
        (let [kafka (deref kafka-atom)
              zookeeper-connect-result
              (ttu/try-connecting-to-zookeeper
                (tc/zookeeper-connect-string kafka))
              kafka-broker-connect-result
              (ttu/try-connecting-to-kafka-broker
                (tc/kafka-bootstrap-servers kafka))
              kafka-connect-connect-result
              (ttu/try-connecting-to-kafka-connect
                (tc/kafka-connect-admin-url kafka))]
          (reset! called true)
          (is (= kafka-connect-port (tkc/rest-port (tc/kafka-connect kafka))))
          (is (not (instance? Exception zookeeper-connect-result)))
          (is (not (instance? Exception kafka-broker-connect-result)))
          (is (not (instance? Exception kafka-connect-connect-result))))))
    (is (true? @called))))

(deftest with-kafka-does-not-include-zookeeper-when-manage-false
  (let [zookeeper (tzk/zookeeper-server)]
    (try
      (let [zookeeper (tzk/start zookeeper)
            zookeeper-connect-string (tzk/connect-string zookeeper)

            kafka-atom (atom nil)
            instantiation-fn
            (tc/with-kafka kafka-atom
              :zookeeper {:manage false}
              :kafka {:zookeeper.connect zookeeper-connect-string})

            called (atom false)]
        (instantiation-fn
          (fn []
            (let [kafka (deref kafka-atom)]
              (reset! called true)
              (is (nil? (tc/zookeeper kafka))))))
        (is (true? @called)))
      (finally
        (tzk/stop zookeeper)))))

(deftest with-kafka-does-not-include-kafka-when-manage-false
  (let [zookeeper (tzk/zookeeper-server)
        kafka (tkb/kafka-broker
                :zookeeper.connect (tzk/connect-string zookeeper))]
    (try
      (tzk/start zookeeper)
      (let [kafka (tkb/start kafka)
            kafka-bootstrap-servers (tkb/bootstrap-servers kafka)

            kafka-atom (atom nil)
            instantiation-fn
            (tc/with-kafka kafka-atom
              :kafka {:manage false}
              :kafka-connect {:bootstrap.servers kafka-bootstrap-servers})

            called (atom false)]
        (instantiation-fn
          (fn []
            (let [kafka (deref kafka-atom)]
              (reset! called true)
              (is (nil? (tc/kafka kafka))))))
        (is (true? @called)))
      (finally
        (tzk/stop kafka)
        (tzk/stop zookeeper)))))

(deftest with-kafka-does-not-include-kafka-connect-when-manage-false
  (let [kafka-atom (atom nil)
        instantiation-fn
        (tc/with-kafka kafka-atom
          :kafka-connect {:manage false})

        called (atom false)]
    (instantiation-fn
      (fn []
        (let [kafka (deref kafka-atom)]
          (reset! called true)
          (is (nil? (tc/kafka-connect kafka))))))
    (is (true? @called))))
