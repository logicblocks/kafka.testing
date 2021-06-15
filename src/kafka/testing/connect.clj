(ns kafka.testing.connect
  (:require
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

(def rest-host-name-key "rest.host.name")
(def rest-port-key "rest.port")
(def bootstrap-servers-key "bootstrap.servers")
(def key-converter-key "key.converter")
(def value-converter-key "value.converter")
(def offset-storage-file-filename-key "offset.storage.file.filename")

(def json-converter-classname "org.apache.kafka.connect.json.JsonConverter")

(def ^:private option-config-keys
  {:hostname            rest-host-name-key
   :port                rest-port-key
   :bootstrap-servers   bootstrap-servers-key
   :key-converter       key-converter-key
   :value-converter     value-converter-key
   :offset-storage-file offset-storage-file-filename-key})

(defn- option-defaults []
  (let [hostname "localhost"
        port (tu/free-port!)
        key-converter json-converter-classname
        value-converter json-converter-classname
        offset-storage-file-filename
        (str (tu/temporary-directory!) File/separator "offsets")]
    {:hostname            hostname
     :port                port
     :key-converter       key-converter
     :value-converter     value-converter
     :offset-storage-file offset-storage-file-filename}))

(defn- config-map [options]
  (let [options (merge (option-defaults) options)]
    (reduce
      (fn [config-map [key value]]
        (assoc (dissoc config-map key)
          (get option-config-keys key key)
          value))
      options
      options)))

(defn- worker-config [config-map]
  (StandaloneConfig. config-map))

(defn- worker-id [config-map]
  (str (UUID/randomUUID) (get config-map rest-port-key)))

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
  (let [config-map (config-map options)
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
     ::config    config-map}))

(defn hostname [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (get-in kafka-connect-server [::config rest-host-name-key])))

(defn port [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (get-in kafka-connect-server [::config rest-port-key])))

(defn admin-url [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (let [config (::config kafka-connect-server)
          hostname (get config rest-host-name-key)
          port (get config rest-port-key)]
      (str "http://" hostname ":" port "/"))))

(defn offset-storage-file [kafka-connect-server]
  (do-if-instance kafka-connect-server
    (get-in kafka-connect-server [::config offset-storage-file-filename-key])))

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
            [:bootstrap-servers
             (tkb/bootstrap-servers @kafka-broker-atom)]
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
