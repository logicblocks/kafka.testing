(ns kafka.testing.broker
  (:require
   [kafka.testing.utils :as tu]
   [kafka.testing.zookeeper :as tzk])
  (:import
   [kafka.server KafkaConfig KafkaServer]
   [org.apache.kafka.common.utils Time]
   [scala Option]))

(def ^:private option-config-keys
  {:hostname                 "host.name"
   :log-directory            "log.dirs"
   :zookeeper-connect-string "zookeeper.connect"})

(defn- ^:private option-defaults []
  {:hostname      "localhost"
   :port          (tu/free-port!)
   :log-directory (tu/temporary-directory!)})

(defn- config-map [options]
  (let [options (merge (option-defaults) options)]
    (reduce
      (fn [config-map [key value]]
        (assoc (dissoc config-map key)
          (get option-config-keys key key)
          value))
      options
      options)))

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

(defn hostname [kafka-broker]
  (-> (::instance kafka-broker)
    (.config)
    (.hostName)))

(defn port [kafka-broker]
  (-> (::instance kafka-broker)
    (.config)
    (.port)))

(defn log-directory [kafka-broker]
  (-> (::instance kafka-broker)
    (.config)
    (.logDirs)
    (.head)))

(defn bootstrap-servers [kafka-broker]
  (str (hostname kafka-broker) ":" (port kafka-broker)))

(defn start [kafka-broker]
  (.startup (::instance kafka-broker))
  kafka-broker)

(defn stop [kafka-broker]
  (.shutdown (::instance kafka-broker))
  (.awaitShutdown (::instance kafka-broker))
  (tu/delete-directory! (log-directory kafka-broker))
  kafka-broker)

(defn with-fresh-kafka-broker [kafka-broker-atom zookeeper-atom & options]
  (fn [run-tests]
    (try
      (reset! kafka-broker-atom
        (apply kafka-broker
          (concat
            [:zookeeper-connect-string
             (tzk/connect-string @zookeeper-atom)]
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
