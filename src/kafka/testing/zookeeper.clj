(ns kafka.testing.zookeeper
  (:import
   [org.apache.curator.test TestingServer]))

(defn zookeeper-server
  ([] (TestingServer. false))
  ([& {:keys [^Integer port]}] (TestingServer. port false)))

(defn start [^TestingServer zookeeper]
  (doto zookeeper
    (.start)))

(defn stop [^TestingServer zookeeper]
  (doto zookeeper
    (.close)))

(defn port [^TestingServer zookeeper]
  (.getPort zookeeper))

(defn connect-string [^TestingServer zookeeper]
  (.getConnectString zookeeper))

(defn data-directory [^TestingServer zookeeper]
  (.getTempDirectory zookeeper))

(defn with-running-zookeeper [zookeeper-atom]
  (fn [run-tests]
    (try
      (reset! zookeeper-atom (start @zookeeper-atom))
      (run-tests)
      (finally
        (reset! zookeeper-atom (stop @zookeeper-atom))))))
