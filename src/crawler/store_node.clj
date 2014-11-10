(ns crawler.store-node
  (require [clojure.core.async :as async]
           [db.neo4j-core :as neo]
           [clojure.tools.logging :refer (error info debug)]
           [crawler.blocks :refer :all]
           [util.workunit :as wu]))

;; This workflow takes a node then makes sure it gets stored
;; handling nodes we have seen before

(def buffers {:callback (async/buffer 10)
              :store    (async/buffer 5)})

(def store-callback-queue (async/chan (:callback buffers)))
(def store-queue (async/chan (:store buffers)))

(defn store-node
  "Given a node, if it has a callback, add to callback queue else just store"
  [node]
  (debug "Storing " node)
  (if
      (wu/unit-metadata-contains? node :callback)
    (async/>!! store-callback-queue node)
    (async/>!! store-queue node)))

(defn enqueue-nodes [node]
  (async/>!! store-queue node)
    (do
      (info "calling: " node)
      ((wu/unit-metadata :callback node) (wu/make-unit node))) )

(defn start []
  (thread-loop #(async/<!! store-callback-queue) enqueue-nodes)
  (thread-loop #(async/<!! store-queue) neo/create-or-update-node)
  [store-callback-queue store-queue]
  )



