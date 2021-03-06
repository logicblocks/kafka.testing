(ns kafka.testing.connect
  (:require
   [clojure.walk :as w]

   [kafka.testing.utils :as tu]
   [kafka.testing.broker :as tkb])
  (:import
   [org.apache.kafka.connect.runtime
    Connect
    Worker]
   [org.apache.kafka.connect.runtime.isolation Plugins]
   [org.apache.kafka.connect.runtime.rest RestServer]
   [org.apache.kafka.connect.runtime.standalone
    StandaloneHerder
    StandaloneConfig]
   [org.apache.kafka.common.utils Time]
   [org.apache.kafka.connect.connector.policy
    NoneConnectorClientConfigOverridePolicy]
   [org.apache.kafka.connect.storage FileOffsetBackingStore]
   [java.io File]
   [java.util UUID]
   [org.apache.kafka.connect.util ConnectUtils]))

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
    {:rest.host.name               hostname
     :rest.port                    port
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

(defn- kafka-cluster-id [config-map]
  (ConnectUtils/lookupKafkaClusterId (worker-config config-map)))

(defn- rest-server [config-map]
  (RestServer. (worker-config config-map)))

(defn- plugins [config-map]
  (Plugins. config-map))

(defn- offset-backing-store [_]
  (FileOffsetBackingStore.))

(defn- connector-client-config-override-policy [_]
  (NoneConnectorClientConfigOverridePolicy.))

(defn- worker [config-map plugins]
  (Worker.
    (worker-id config-map)
    Time/SYSTEM
    plugins
    (worker-config config-map)
    (offset-backing-store config-map)
    (connector-client-config-override-policy config-map)))

(defn- herder [config-map worker]
  (StandaloneHerder.
    worker
    (kafka-cluster-id config-map)
    (connector-client-config-override-policy config-map)))

(defn- connect [_ herder rest-server]
  (Connect. herder rest-server))

(defn kafka-connect-server [& {:as options}]
  (let [options (defaulted-options options)
        config-map (string-keyed-config-map options)
        rest-server (rest-server config-map)
        plugins (plugins config-map)
        worker (worker config-map plugins)
        herder (herder config-map worker)
        connect (connect config-map herder rest-server)]
    {::instances {::connect     connect
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
    (get-in kafka-connect-server [::config :rest.host.name])))

(defn rest-port [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (get-in kafka-connect-server [::config :rest.port])))

(defn admin-url [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (let [config (::config kafka-connect-server)
          hostname (get config :rest.host.name)
          port (get config :rest.port)]
      (str "http://" hostname ":" port "/"))))

(defn offset-storage-file-filename [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (get-in kafka-connect-server [::config :offset.storage.file.filename])))

(defn start [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (.compareAndSwapWithDelegatingLoader
      (get-in kafka-connect-server [::instances ::plugins]))
    (.initializeServer
      (get-in kafka-connect-server [::instances ::rest-server]))
    (.start
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
