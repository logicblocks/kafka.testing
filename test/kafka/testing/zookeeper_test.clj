(ns kafka.testing.zookeeper-test
  (:require
   [clojure.test :refer :all]

   [kafka.testing.logging]
   [kafka.testing.zookeeper :as tzk]
   [kafka.testing.test-utils :as tu]))

(deftest zookeeper-server-uses-a-random-port
  (let [zookeeper (tzk/zookeeper-server)]
    (is (not (= 2181 (tzk/port zookeeper))))))

(deftest zookeeper-server-uses-the-specified-port
  (let [zookeeper (tzk/zookeeper-server :port 2182)]
    (is (= 2182 (tzk/port zookeeper)))))

(deftest zookeeper-server-does-not-start-the-server
  (let [zookeeper (tzk/zookeeper-server)
        connect-string (tzk/connect-string zookeeper)
        connect-result (tu/try-connecting-to-zookeeper connect-string)]
    (is (instance? IllegalStateException connect-result))))

(deftest start-starts-the-provided-zookeeper-server
  (let [zookeeper (tzk/zookeeper-server)
        zookeeper (tzk/start zookeeper)
        data-directory (tzk/data-directory zookeeper)
        connect-string (tzk/connect-string zookeeper)
        connect-result (tu/try-connecting-to-zookeeper connect-string)]
    (is (true? (tu/path-exists? data-directory)))
    (is (not (instance? IllegalStateException connect-result)))))

(deftest stop-stops-the-provided-zookeeper-server
  (let [zookeeper (tzk/zookeeper-server)
        zookeeper (tzk/start zookeeper)
        zookeeper (tzk/stop zookeeper)
        data-directory (tzk/data-directory zookeeper)
        connect-string (tzk/connect-string zookeeper)
        connect-result (tu/try-connecting-to-zookeeper connect-string)]
    (is (false? (tu/path-exists? data-directory)))
    (is (instance? IllegalStateException connect-result))))

(deftest with-fresh-zookeeper-instantiates-new-zookeeper
  (let [zookeeper-atom (atom nil)
        instantiation-fn (tzk/with-fresh-zookeeper zookeeper-atom)

        instance-before-invocations (atom nil)
        instance-during-first-invocation (atom nil)
        instance-after-first-invocation (atom nil)
        instance-during-second-invocation (atom nil)
        instance-after-second-invocation (atom nil)]
    (reset! instance-before-invocations @zookeeper-atom)
    (instantiation-fn
      (fn []
        (reset! instance-during-first-invocation @zookeeper-atom)))
    (reset! instance-after-first-invocation @zookeeper-atom)
    (instantiation-fn
      (fn []
        (reset! instance-during-second-invocation @zookeeper-atom)))
    (reset! instance-after-second-invocation @zookeeper-atom)

    (is (nil? @instance-before-invocations))
    (is (not (nil? @instance-during-first-invocation)))
    (is (nil? @instance-after-first-invocation))
    (is (not (nil? @instance-during-second-invocation)))
    (is (nil? @instance-after-second-invocation))
    (is (not (= @instance-after-first-invocation
               @instance-during-second-invocation)))))

(deftest with-running-zookeeper-manages-zookeeper-lifecycle
  (let [zookeeper (tzk/zookeeper-server)
        zookeeper-atom (atom zookeeper)
        lifecycle-fn (tzk/with-running-zookeeper zookeeper-atom)
        connect-string (tzk/connect-string zookeeper)
        data-directory (tzk/data-directory zookeeper)

        connect-result-before (tu/try-connecting-to-zookeeper connect-string)
        data-directory-exists-before (tu/path-exists? data-directory)

        connect-result-during-atom (atom nil)
        data-directory-exists-during-atom (atom nil)
        _ (lifecycle-fn
            (fn []
              (reset! data-directory-exists-during-atom
                (tu/path-exists? data-directory))
              (reset! connect-result-during-atom
                (tu/try-connecting-to-zookeeper connect-string))))
        connect-result-during (deref connect-result-during-atom)
        data-directory-exists-during (deref data-directory-exists-during-atom)

        connect-result-after (tu/try-connecting-to-zookeeper connect-string)
        data-directory-exists-after (tu/path-exists? data-directory)]
    (is (instance? IllegalStateException connect-result-before))
    (is (true? data-directory-exists-before))

    (is (not (instance? IllegalStateException connect-result-during)))
    (is (true? data-directory-exists-during))

    (is (instance? IllegalStateException connect-result-after))
    (is (false? data-directory-exists-after))))
