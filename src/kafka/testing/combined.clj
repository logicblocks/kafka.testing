(ns kafka.testing.combined
  (:require
   [kafka.testing.zookeeper :as tzk]
   [kafka.testing.broker :as tkb]
   [kafka.testing.connect :as tkc]))

(defn- options-vec [options]
  (flatten (seq (or options {}))))

(defn zookeeper [kafka-combined]
  (get-in kafka-combined [::instances ::zookeeper]))

(defn zookeeper-connect-string [kafka-combined]
  (tzk/connect-string (zookeeper kafka-combined)))

(defn kafka [kafka-combined]
  (get-in kafka-combined [::instances ::kafka]))

(defn kafka-bootstrap-servers [kafka-combined]
  (tkb/bootstrap-servers (kafka kafka-combined)))

(defn kafka-connect [kafka-combined]
  (get-in kafka-combined [::instances ::kafka-connect]))

(defn kafka-connect-admin-url [kafka-combined]
  (tkc/admin-url (kafka-connect kafka-combined)))

(defn with-kafka [kafka-atom & {:as options}]
  (fn [run-tests]
    (try
      (let [zookeeper-defaults {:manage true}
            zookeeper-options (merge zookeeper-defaults (:zookeeper options))
            zookeeper (when (:manage zookeeper-options)
                        (tzk/start
                          (apply tzk/zookeeper-server
                            (options-vec (dissoc zookeeper-options :manage)))))
            zookeeper-connect-string (tzk/connect-string zookeeper)

            kafka-defaults {:manage                   true
                            :zookeeper-connect-string zookeeper-connect-string}
            kafka-options (merge kafka-defaults (:kafka options))
            kafka (when (:manage kafka-options)
                    (tkb/start
                      (apply tkb/kafka-broker
                        (options-vec (dissoc kafka-options :manage)))))
            kafka-bootstrap-servers (tkb/bootstrap-servers kafka)

            connect-defaults {:manage            true
                              :bootstrap-servers kafka-bootstrap-servers}
            connect-options (merge connect-defaults (:kafka-connect options))
            connect (when (:manage connect-options)
                      (tkc/start
                        (apply tkc/kafka-connect-server
                          (options-vec (dissoc connect-options :manage)))))]
        (reset! kafka-atom
          {::instances {::zookeeper     zookeeper
                        ::kafka         kafka
                        ::kafka-connect connect}
           ::config    options})
        (run-tests))
      (finally
        (tkc/stop (kafka-connect @kafka-atom))
        (tkb/stop (kafka @kafka-atom))
        (tzk/stop (zookeeper @kafka-atom))
        (reset! kafka-atom nil)))))
