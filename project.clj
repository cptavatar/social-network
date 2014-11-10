(defproject loaders "0.1.0-SNAPSHOT"
            :description "Functionality load dealer data into neo4j/mongodb"
            :url "http://example.com/FIXME"
            :license {:name "Eclipse Public License"
                      :url  "http://www.eclipse.org/legal/epl-v10.html"}
            :dependencies [[org.clojure/clojure "1.6.0"],
                           [org.clojure/data.xml "0.0.7"],
                           [clojurewerkz/neocons "3.0.0"],
                           [twitter-api "0.7.5"],
                           [org.clojure/core.async "0.1.338.0-5c5012-alpha"],
                           [clj-time "0.8.0"],
                           [org.clojure/tools.logging "0.3.0"]
                           [ch.qos.logback/logback-classic "1.1.2"]
                           [org.clojure/core.cache "0.6.4"]
                           [metrics-clojure "2.2.0"]]
            :main core)
