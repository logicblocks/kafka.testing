(ns kafka.testing.zookeeper
  (:import
   [org.apache.curator.test TestingServer]))

(defn zookeeper-server
  ([]
   {::instance (TestingServer. false)})
  ([& {:keys [^Integer port]}]
   {::instance (TestingServer. port false)}))

(defn start [zookeeper]
  (.start (::instance zookeeper))
  zookeeper)

(defn stop [zookeeper]
  (.close (::instance zookeeper))
  zookeeper)

(defn port [zookeeper]
  (.getPort (::instance zookeeper)))

(defn connect-string [zookeeper]
  (.getConnectString (::instance zookeeper)))

(defn data-directory [zookeeper]
  (.getTempDirectory (::instance zookeeper)))

(defn with-fresh-zookeeper [zookeeper-atom]
  (fn [run-tests]
    (try
      (reset! zookeeper-atom (zookeeper-server))
      (run-tests)
      (finally
        (reset! zookeeper-atom nil)))))

(defn with-running-zookeeper [zookeeper-atom]
  (fn [run-tests]
    (try
      (reset! zookeeper-atom (start @zookeeper-atom))
      (run-tests)
      (finally
        (reset! zookeeper-atom (stop @zookeeper-atom))))))
