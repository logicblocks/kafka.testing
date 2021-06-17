(ns kafka.testing.broker
  (:require
   [clojure.walk :as w]

   [kafka.testing.utils :as tu]
   [kafka.testing.zookeeper :as tzk])
  (:import
   [kafka.server KafkaConfig KafkaServer]
   [org.apache.kafka.common.utils Time]
   [scala Option]))

(defmacro ^:private do-if-instance [kafka-broker & body]
  `(when (and ~kafka-broker (::instance ~kafka-broker))
     ~@body))

(defn- ^:private option-defaults []
  {:host.name "localhost"
   :port      (tu/free-port!)
   :log.dir   (tu/temporary-directory!)})

(defn- config-map [options]
  (let [options (merge (option-defaults) options)]
    (w/stringify-keys options)))

(defn- kafka-config [config-map]
  (KafkaConfig/fromProps
    (tu/properties config-map)))

(defn kafka-broker [& {:as options}]
  (let [config-map (config-map options)
        instance (KafkaServer.
                   (kafka-config config-map)
                   Time/SYSTEM
                   (Option/empty)
                   false)]
    {::instance instance
     ::config   config-map}))

(defn host-name [kafka-broker]
  (do-if-instance kafka-broker
    (-> (::instance kafka-broker)
      (.config)
      (.hostName))))

(defn port [kafka-broker]
  (do-if-instance kafka-broker
    (-> (::instance kafka-broker)
      (.config)
      (.port))))

(defn log-directory [kafka-broker]
  (do-if-instance kafka-broker
    (-> (::instance kafka-broker)
      (.config)
      (.logDirs)
      (.head))))

(defn bootstrap-servers [kafka-broker]
  (do-if-instance kafka-broker
    (str (host-name kafka-broker) ":" (port kafka-broker))))

(defn start [kafka-broker]
  (do-if-instance kafka-broker
    (.startup (::instance kafka-broker)))
  kafka-broker)

(defn stop [kafka-broker]
  (do-if-instance kafka-broker
    (.shutdown (::instance kafka-broker))
    (.awaitShutdown (::instance kafka-broker))
    (tu/delete-directory! (log-directory kafka-broker)))
  kafka-broker)

(defn with-fresh-kafka-broker [kafka-broker-atom zookeeper-atom & options]
  (fn [run-tests]
    (try
      (reset! kafka-broker-atom
        (apply kafka-broker
          (concat
            [:zookeeper.connect (tzk/connect-string @zookeeper-atom)]
            options)))
      (run-tests)
      (finally
        (reset! kafka-broker-atom nil)))))

(defn with-running-kafka-broker [kafka-broker-atom]
  (fn [run-tests]
    (try
      (reset! kafka-broker-atom (start @kafka-broker-atom))
      (run-tests)
      (finally
        (reset! kafka-broker-atom (stop @kafka-broker-atom))))))
