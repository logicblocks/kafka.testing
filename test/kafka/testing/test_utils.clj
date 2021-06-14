(ns kafka.testing.test-utils
  (:require
   [clojure.java.io :as io]))

(defn path-exists? [path]
  (.exists (io/file path)))
