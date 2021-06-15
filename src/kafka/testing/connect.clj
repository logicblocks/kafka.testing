(ns kafka.testing.connect
  (:require
   [kafka.testing.utils :as tu])
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
   [org.apache.kafka.connect.storage MemoryOffsetBackingStore]
   [org.apache.kafka.connect.util ConnectUtils]
   [java.io File]))

(def ^:private option-config-keys
  {:hostname            "rest.host.name"
   :port                "rest.port"
   :bootstrap-servers   "bootstrap.servers"
   :key-converter       "key.converter"
   :value-converter     "value.converter"
   :offset-storage-file "offset.storage.file.filename"})

(defn- option-defaults []
  (let [hostname "localhost"
        port (tu/free-port!)
        key-converter "org.apache.kafka.connect.json.JsonConverter"
        value-converter "org.apache.kafka.connect.json.JsonConverter"
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

(defn kafka-connect-server [& {:as options}]
  (let [config-map (config-map options)
        worker-config (worker-config config-map)
        kafka-cluster-id (ConnectUtils/lookupKafkaClusterId worker-config)
        rest-server (RestServer. worker-config)
        rest-advertised-url (.advertisedUrl rest-server)
        worker-id (str (.getHost rest-advertised-url) ":"
                    (.getPort rest-advertised-url))
        plugins (Plugins. config-map)
        offset-backing-store
        (MemoryOffsetBackingStore.)
        connector-client-config-override-policy
        (NoneConnectorClientConfigOverridePolicy.)
        worker (Worker.
                 worker-id
                 Time/SYSTEM
                 plugins
                 worker-config
                 offset-backing-store
                 connector-client-config-override-policy)
        herder (StandaloneHerder.
                 worker
                 kafka-cluster-id
                 connector-client-config-override-policy)
        connect (Connect. herder rest-server)]
    {::instances {::connect     connect
                  ::rest-server rest-server
                  ::plugins     plugins
                  ::worker      worker
                  ::herder      herder}
     ::config    config-map}))

(defn hostname [kafka-connect-server]
  (get-in kafka-connect-server [::config "rest.host.name"]))

(defn port [kafka-connect-server]
  (get-in kafka-connect-server [::config "rest.port"]))

(defn admin-url [kafka-connect-server]
  (let [config (::config kafka-connect-server)
        hostname (get config "rest.host.name")
        port (get config "rest.port")]
    (str "http://" hostname ":" port "/")))

(defn offset-storage-file [kafka-connect-server]
  (get-in kafka-connect-server [::config "offset.storage.file.filename"]))

(defn start [kafka-connect-server]
  (.compareAndSwapWithDelegatingLoader
    (get-in kafka-connect-server [::instances ::plugins]))
  (.initializeServer
    (get-in kafka-connect-server [::instances ::rest-server]))
  (.start
    (get-in kafka-connect-server [::instances ::connect]))
  kafka-connect-server)

(defn stop [kafka-connect-server]
  (let [^Connect connect (get-in kafka-connect-server [::instances ::connect])]
    (.stop connect)
    (.awaitStop connect))
  kafka-connect-server)
