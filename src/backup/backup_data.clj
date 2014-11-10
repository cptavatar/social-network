(ns backup.backup-data
  (require [clojure.core.async :as async]

           [db.neo4j-core :as neo]
           [clojure.tools.logging :refer (error info debug)]
           [crawler.blocks :refer :all]))

;; Backup core data as edn so it can be passed around, reloaded, etc.
;; WARNING - currently fails if "backup/ dir doesn't exist - need to add step
;; to create

(defn write-obj-to-file [name seq]
  (println "backing up " name)
  (with-open [wtr (clojure.java.io/writer (str "backup/" name ".edn"))]
    (doseq [obj seq]
      (.write wtr (str (pr-str (dissoc obj :_id)) "\n")))))


(defn backup-relationships []
  (write-obj-to-file "relationships" (neo/find-all-relationships))
  )



(defn backup []

  (backup-relationships)
  )