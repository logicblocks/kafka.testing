(ns kafka.testing.zookeeper-test
  (:require
   [clojure.test :refer :all]
   [clojure.java.io :as io]

   [zookeeper :as zk]

   [kafka.testing.zookeeper :as tzk]))

(defn try-connect [connect-string]
  (try
    (zk/connect connect-string :timeout-msec 50)
    (catch IllegalStateException e e)))

(defn check-existence [path]
  (.exists (io/file path)))

(deftest creates-a-zookeeper-server-on-a-random-port
  (let [zookeeper (tzk/zookeeper-server)]
    (is (not (= 2181 (tzk/port zookeeper))))))

(deftest creates-a-zookeeper-server-on-the-specified-port
  (let [zookeeper (tzk/zookeeper-server :port 2182)]
    (is (= 2182 (tzk/port zookeeper)))))

(deftest creates-an-unstarted-zookeeper-server
  (let [zookeeper (tzk/zookeeper-server)
        connect-string (tzk/connect-string zookeeper)
        connect-result (try-connect connect-string)]
    (is (instance? IllegalStateException connect-result))))

(deftest starts-the-provided-zookeeper-server
  (let [zookeeper (tzk/zookeeper-server)
        zookeeper (tzk/start zookeeper)
        data-directory (tzk/data-directory zookeeper)
        connect-string (tzk/connect-string zookeeper)
        connect-result (try-connect connect-string)]
    (is (true? (check-existence data-directory)))
    (is (not (instance? IllegalStateException connect-result)))))

(deftest stops-the-provided-zookeeper-server
  (let [zookeeper (tzk/zookeeper-server)
        zookeeper (tzk/start zookeeper)
        zookeeper (tzk/stop zookeeper)
        data-directory (tzk/data-directory zookeeper)
        connect-string (tzk/connect-string zookeeper)
        connect-result (try-connect connect-string)]
    (is (false? (check-existence data-directory)))
    (is (instance? IllegalStateException connect-result))))

(deftest creates-function-to-manage-zookeeper-lifecycle
  (let [zookeeper (tzk/zookeeper-server)
        zookeeper-atom (atom zookeeper)
        lifecycle-fn (tzk/with-running-zookeeper zookeeper-atom)
        connect-string (tzk/connect-string zookeeper)
        data-directory (tzk/data-directory zookeeper)

        connect-result-before (try-connect connect-string)
        data-directory-exists-before (check-existence data-directory)

        connect-result-during-atom (atom nil)
        data-directory-exists-during-atom (atom nil)
        _ (lifecycle-fn
            (fn []
              (reset! data-directory-exists-during-atom
                (check-existence data-directory))
              (reset! connect-result-during-atom
                (try-connect connect-string))))
        connect-result-during (deref connect-result-during-atom)
        data-directory-exists-during (deref data-directory-exists-during-atom)

        connect-result-after (try-connect connect-string)
        data-directory-exists-after (check-existence data-directory)]
    (is (instance? IllegalStateException connect-result-before))
    (is (true? data-directory-exists-before))

    (is (not (instance? IllegalStateException connect-result-during)))
    (is (true? data-directory-exists-during))

    (is (instance? IllegalStateException connect-result-after))
    (is (false? data-directory-exists-after))))
