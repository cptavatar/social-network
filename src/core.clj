(ns core
  (:gen-class)
  (:require
    [db.neo4j-core :as neo]
    [crawler.master :as master]
    [crawler.relation-crawler :as relations]
    [crawler.blocks :refer :all]
    [clojure.core.async :as async]
    [clojure.tools.logging :refer (info error log)]
    [backup.backup-data :as backup-data]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Core - loader entrypoint
; - initialize neo4j subsystem
; - for now, uncomment the operation you would like to run :)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def server "localhost")

(defn -main
  "Application entry point"
  [& args]
  (neo/neo-init server 7474)

  ; kick off the dealer profile update process
  ;(master/refresh-dealer-twitter-info)

  ;(backup-data/backup)

  ;wipe out the db, re-initialize from dealerInfo.edn file
  (master/nuke-and-reload)

  ;crawl for friends/followers -
  ;currently needs to be done as separate step from reload
  ;(master/start-crawler)

  ;(thread-loop #(async/<!! relations/lookup-queue) #(println "adding to queue" %1))
  ;(relations/load-data)
  ;(Thread/sleep 1500000)

  )
