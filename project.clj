(defproject io.logicblocks/kafka.testing "0.0.2-RC1"
  :description "A Clojure library to help with tests involving Kafka."
  :url "https://github.com/logicblocks/kafka.testing"

  :license {:name "The MIT License"
            :url  "https://opensource.org/licenses/MIT"}

  :plugins [[lein-cloverage "1.1.2"]
            [lein-shell "0.5.0"]
            [lein-ancient "0.6.15"]
            [lein-changelog "0.3.2"]
            [lein-eftest "0.5.9"]
            [lein-codox "0.10.7"]
            [lein-cljfmt "0.6.7"]
            [lein-kibit "0.1.8"]
            [lein-bikeshed "0.5.2"]
            [jonase/eastwood "0.3.11"]]

  :dependencies [[org.apache.curator/curator-test "5.1.0"]
                 [org.apache.kafka/kafka_2.13 "2.8.0"]
                 [org.apache.kafka/connect-runtime "2.8.0"
                  :exclusions [org.slf4j/slf4j-log4j12]]]

  :profiles
  {:shared
   {:dependencies   [[org.clojure/clojure "1.10.3"]

                     [org.slf4j/jcl-over-slf4j "1.7.30"]
                     [org.slf4j/jul-to-slf4j "1.7.30"]
                     [org.slf4j/log4j-over-slf4j "1.7.30"]
                     [ch.qos.logback/logback-classic "1.2.3"]

                     [nrepl "0.8.3"]
                     [eftest "0.5.9"]

                     [zookeeper-clj "0.9.4"]
                     [fundingcircle/jackdaw "0.6.0"]
                     [org.sourcelab/kafka-connect-client "3.1.1"]]
    :resource-paths ["test_resources"]
    :aot            :all}
   :dev
   [:shared {:source-paths ["dev"]
             :eftest       {:multithread? false}}]
   :test
   [:shared {:eftest {:multithread? false}}]
   :prerelease
   {:release-tasks
    [["shell" "git" "diff" "--exit-code"]
     ["change" "version" "leiningen.release/bump-version" "rc"]
     ["change" "version" "leiningen.release/bump-version" "release"]
     ["vcs" "commit" "Pre-release version %s [skip ci]"]
     ["vcs" "tag"]
     ["deploy"]]}
   :release
   {:release-tasks
    [["shell" "git" "diff" "--exit-code"]
     ["change" "version" "leiningen.release/bump-version" "release"]
     ["codox"]
     ["changelog" "release"]
     ["shell" "sed" "-E" "-i.bak" "s/\"[0-9]+\\.[0-9]+\\.[0-9]+\"/\"${:version}\"/g" "README.md"]
     ["shell" "rm" "-f" "README.md.bak"]
     ["shell" "git" "add" "."]
     ["vcs" "commit" "Release version %s [skip ci]"]
     ["vcs" "tag"]
     ["deploy"]
     ["change" "version" "leiningen.release/bump-version" "patch"]
     ["change" "version" "leiningen.release/bump-version" "rc"]
     ["change" "version" "leiningen.release/bump-version" "release"]
     ["vcs" "commit" "Pre-release version %s [skip ci]"]
     ["vcs" "tag"]
     ["vcs" "push"]]}}

  :cloverage
  {:ns-exclude-regex [#"^user"]}

  :codox
  {:namespaces  [#"^kafka\.testing"]
   :metadata    {:doc/format :markdown}
   :output-path "docs"
   :doc-paths   ["docs"]
   :source-uri  "https://github.com/logicblocks/kafka.testing/blob/{version}/{filepath}#L{line}"}

  :cljfmt {:indents ^:replace {#".*" [[:inner 0]]}}

  :eastwood {:config-files ["config/linter.clj"]}

  :deploy-repositories
  {"releases"  {:url "https://repo.clojars.org" :creds :gpg}
   "snapshots" {:url "https://repo.clojars.org" :creds :gpg}})
