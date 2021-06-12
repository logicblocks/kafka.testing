(ns kafka.testing.utils
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
      (.toString (UUID/randomUUID))
      (into-array FileAttribute []))
    (.toAbsolutePath)
    (.toString)))

(defn properties [map]
  (reduce
    (fn [^Properties properties [key value]]
      (doto properties
        (.put (name key) (str value))))
    (Properties.)
    (seq map)))
