(ns kafka.testing.zookeeper
  (:require [kafka.testing.utils :as tu])
  (:import
   [org.apache.curator.test TestingServer]))

(defmacro ^:private do-if-instance [zookeeper & body]
  `(when (and ~zookeeper (::instance ~zookeeper))
     ~@body))

(defn zookeeper-server [& {:as options}]
  (let [^Integer port (get options :port (tu/free-port!))]
    {::instance (TestingServer. port false)}))

(defn start [zookeeper]
  (do-if-instance zookeeper
    (.start (::instance zookeeper)))
  zookeeper)

(defn stop [zookeeper]
  (do-if-instance zookeeper
    (.close (::instance zookeeper)))
  zookeeper)

(defn port [zookeeper]
  (do-if-instance zookeeper
    (.getPort (::instance zookeeper))))

(defn connect-string [zookeeper]
  (do-if-instance zookeeper
    (.getConnectString (::instance zookeeper))))

(defn data-directory [zookeeper]
  (do-if-instance zookeeper
    (.getTempDirectory (::instance zookeeper))))

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
