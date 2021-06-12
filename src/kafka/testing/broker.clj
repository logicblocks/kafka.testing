(ns kafka.testing.broker
  (:require
   [kafka.testing.utils :as tu])
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
   :ip            "0.0.0.0"
   :port          (tu/free-port!)
   :log-directory (tu/temporary-directory!)})

(defn- kafka-config [options]
  (let [config-map
        (reduce
          (fn [config-map [key value]]
            (assoc (dissoc config-map key)
              (get option-config-keys key key)
              value))
          options
          options)]
    (KafkaConfig/fromProps
      (tu/properties config-map))))

(defn kafka-broker [& {:as options}]
  (KafkaServer.
    (kafka-config
      (merge (option-defaults) options))
    Time/SYSTEM
    (Option/empty)
    false))

(defn hostname [^KafkaServer broker]
  (-> broker
    (.config)
    (.hostName)))

(defn port [^KafkaServer broker]
  (-> broker
    (.config)
    (.port)))

(defn log-directory [^KafkaServer broker]
  (-> broker
    (.config)
    (.logDirs)
    (.head)))

(defn bootstrap-servers [^KafkaServer broker]
  (str (hostname broker) ":" (port broker)))

(defn start [^KafkaServer broker]
  (doto broker
    (.startup)))

(defn stop [^KafkaServer broker]
  (doto broker
    (.shutdown)
    (.awaitShutdown))
  
  broker)
