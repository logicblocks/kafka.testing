(ns kafka.testing.utils
  (:require
   [clojure.java.io :as io])
  (:import
   [java.net ServerSocket]
   [java.util Properties UUID]
   [java.nio.file Files]
   [java.nio.file.attribute FileAttribute]))

(defn free-port! []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

(defn temporary-directory! []
  (->
    (Files/createTempDirectory
      (str (UUID/randomUUID))
      (into-array FileAttribute []))
    (.toAbsolutePath)
    (.toString)))

(defn delete-directory! [path]
  (let [file (io/file path)]
    (when (.isDirectory file)
      (doseq [child (.listFiles file)]
        (delete-directory! child)))
    (io/delete-file file)))

(defn properties [m]
  (reduce
    (fn [^Properties properties [key value]]
      (doto properties
        (.put (name key) (str value))))
    (Properties.)
    (seq m)))
