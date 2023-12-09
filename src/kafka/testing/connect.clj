(ns kafka.testing.connect
  (:require
   [clojure.string :as string]
   [clojure.walk :as w]

   [kafka.testing.utils :as tu]
   [kafka.testing.broker :as tkb])
  (:import
   [org.apache.kafka.connect.json JsonConverter JsonConverterConfig]
   [org.apache.kafka.connect.runtime
    Connect
    Worker WorkerConfig]
   [org.apache.kafka.connect.runtime.isolation Plugins]
   [org.apache.kafka.connect.runtime.rest ConnectRestServer RestClient]
   [org.apache.kafka.connect.runtime.standalone
    StandaloneHerder
    StandaloneConfig]
   [org.apache.kafka.common.utils Time]
   [org.apache.kafka.connect.connector.policy
    NoneConnectorClientConfigOverridePolicy]
   [org.apache.kafka.connect.storage FileOffsetBackingStore]
   [java.io File]
   [java.util UUID]))

(defmacro ^:private do-if-instance [kafka-connect-server & body]
  `(when (and ~kafka-connect-server
           (get-in ~kafka-connect-server [::instances ::plugins])
           (get-in ~kafka-connect-server [::instances ::rest-server])
           (get-in ~kafka-connect-server [::instances ::connect])
           (get-in ~kafka-connect-server [::config]))
     ~@body))

(def json-converter-classname "org.apache.kafka.connect.json.JsonConverter")

(defn- option-defaults []
  (let [hostname "localhost"
        port (tu/free-port!)
        key-converter json-converter-classname
        value-converter json-converter-classname
        offset-storage-file-filename
        (str (tu/temporary-directory!) File/separator "offsets")]
    {:listeners                    (str "HTTP://" hostname ":" port)
     :key.converter                key-converter
     :value.converter              value-converter
     :offset.storage.file.filename offset-storage-file-filename}))

(defn defaulted-options [options]
  (merge (option-defaults) options))

(defn- string-keyed-config-map [options]
  (w/stringify-keys options))

(defn- keyword-keyed-config-map [options]
  (w/keywordize-keys options))

(defn- worker-config [config-map]
  (StandaloneConfig. config-map))

(defn- worker-id [config-map]
  (str (UUID/randomUUID) (get config-map :rest.port)))

(defn- kafka-cluster-id [^WorkerConfig config]
  (.kafkaClusterId config))

(defn- rest-client [config]
  (RestClient. config))

(defn- rest-server [rest-client config-map]
  (ConnectRestServer. nil rest-client config-map))

(defn- plugins [config-map]
  (Plugins. config-map))

(defn- offset-backing-store [^Plugins plugins ^WorkerConfig config]
  (let [converter
        (.newInternalConverter plugins
          true
          (.getName JsonConverter)
          {JsonConverterConfig/SCHEMAS_ENABLE_CONFIG "false"})
        store (FileOffsetBackingStore. converter)]
    (.configure store config)
    store))

(defn- connector-client-config-override-policy []
  (NoneConnectorClientConfigOverridePolicy.))

(defn- worker [plugins config config-map]
  (Worker.
    (worker-id config-map)
    Time/SYSTEM
    plugins
    config
    (offset-backing-store plugins config)
    (connector-client-config-override-policy)))

(defn- herder [worker config]
  (StandaloneHerder.
    worker
    (kafka-cluster-id config)
    (connector-client-config-override-policy)))

(defn- connect [herder rest-server]
  (Connect. herder rest-server))

(defn kafka-connect-server [& {:as options}]
  (let [options (defaulted-options options)
        config-map (string-keyed-config-map options)
        config (worker-config config-map)
        rest-client (rest-client config)
        rest-server (rest-server rest-client config-map)
        plugins (plugins config-map)
        worker (worker plugins config config-map)
        herder (herder worker config)
        connect (connect herder rest-server)]
    {::instances {::connect     connect
                  ::rest-client rest-client
                  ::rest-server rest-server
                  ::plugins     plugins
                  ::worker      worker
                  ::herder      herder}
     ::config    (keyword-keyed-config-map options)}))

(defn configuration [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (::config kafka-connect-server)))

(defn rest-host-name [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (let [config (::config kafka-connect-server)
          listeners (get config :listeners)
          listener (first (string/split listeners #","))
          listener (string/replace listener "HTTP://" "")
          host-name (first (string/split listener #":"))]
      host-name)))

(defn rest-port [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (let [config (::config kafka-connect-server)
          listeners (get config :listeners)
          listener (first (string/split listeners #","))
          listener (string/replace listener "HTTP://" "")
          port (second (string/split listener #":"))]
      (Integer/parseInt port))))

(defn admin-url [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (let [config (::config kafka-connect-server)
          listeners (get config :listeners)
          listener (first (string/split listeners #","))]
      (string/lower-case listener))))

(defn offset-storage-file-filename [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (get-in kafka-connect-server [::config :offset.storage.file.filename])))

(defn start [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (.compareAndSwapWithDelegatingLoader
      ^Plugins
      (get-in kafka-connect-server [::instances ::plugins]))
    (.initializeServer
      ^ConnectRestServer
      (get-in kafka-connect-server [::instances ::rest-server]))
    (.start
      ^Connect
      (get-in kafka-connect-server [::instances ::connect])))
  kafka-connect-server)

(defn stop [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (let [^Connect connect
          (get-in kafka-connect-server [::instances ::connect])]
      (.stop connect)
      (.awaitStop connect)))
  kafka-connect-server)

(defn with-fresh-kafka-connect-server
  [kafka-connect-server-atom kafka-broker-atom & options]
  (fn [run-tests]
    (try
      (reset! kafka-connect-server-atom
        (apply kafka-connect-server
          (concat
            [:bootstrap.servers (tkb/bootstrap-servers @kafka-broker-atom)]
            options)))
      (run-tests)
      (finally
        (reset! kafka-connect-server-atom nil)))))

(defn with-running-kafka-connect-server [kafka-connect-server-atom]
  (fn [run-tests]
    (try
      (reset! kafka-connect-server-atom
        (start @kafka-connect-server-atom))
      (run-tests)
      (finally
        (reset! kafka-connect-server-atom
          (stop @kafka-connect-server-atom))))))
