(ns crawler.master
  (require [clojure.core.async :as async]
           [db.neo4j-core :as neo]
           [crawler.store-node :as store-node]
           [crawler.update-node :as update-node]
           [crawler.relation-crawler :as relations]
           [util.socialite :as sc]
           [loader.load-data :as loader]
           [util.edn-helper :as edn-helper]
           [crawler.blocks :refer (wait shutdown-watch)]
           [clojure.tools.logging :refer (debug info error)]
           [metrics.reporters.console :as mc]))


(defn load-data []
  ;(enricher/load-data)
  ;(update-node/load-data)
  (relations/load-data))



(defn launch-buffer-status [buf-map name]
  (async/thread
    (loop []
      (info "Buffer status for " name
            (map (fn [x] (str x " " (.count (buf-map x)) "(" (.full? (buf-map x)) ") ")) (keys buf-map)))
      (wait 30000)
      (recur)
      )
    ))

(defn nuke-and-reload
  "Start up the workflow to to wipe out data fromt he system, reload"
  []
  (neo/neo-reset-data)
  (loader/start (edn-helper/load-from-edn "conf/edn/dealerInfo.edn" sc/create-socialite-legacy)))


(defn start-crawler
  "Start the crawler process. First, initilize the work queues, then launch the queue consumers"
  []
  (load-data)
  (launch-buffer-status relations/buffers "relations-crawler")
  (launch-buffer-status store-node/buffers "store-node")
  (launch-buffer-status update-node/buffers "update-node")
  (mc/start (mc/reporter {}) 30)
  (shutdown-watch
    (concat
      (store-node/start)
      (update-node/start)
      (relations/start))))
