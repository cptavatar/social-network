(ns crawler.relation-crawler
  (require [clojure.core.async :as async]
           [twit.twitter-core :as to]
           [db.neo4j-core :as neo]
           [clj-time.core :as ct]
           [clj-time.coerce :as ctc]
           [crawler.store-node :as sn]
           [crawler.update-node :as un]
           [crawler.blocks :refer :all]
           [util.assertion :refer :all]
           [util.workunit :as wu]
           [clojure.core.async.impl.buffers :refer :all]
           [clojure.tools.logging :refer (debug info error)]))

; This module is for looking the followers for twitter users
; and then persisting the results

;; How many relations before you get put in the slow lane
(def max-relations 10000)

(def buffers {
               :lookup           (async/buffer 200)
               :lookup-relations (async/buffer 500)
              :friend         (async/buffer 15)
              :follower       (async/buffer 15)
              :slow-friend    (async/buffer 1000)
              :slow-follower  (async/buffer 1000)
              :store-relation (async/buffer 50000)})

(def lookup-queue (async/chan (:lookup buffers)))

(def lookup-relations-queue (async/chan (:callback buffers)))

(def friend-lookup-queue (async/chan (:friend buffers)))
(def slow-friend-lookup-queue (async/chan (:slow-friend buffers)))
(def follower-lookup-queue (async/chan (:follower buffers)))
(def slow-follower-lookup-queue (async/chan (:slow-follower buffers)))
(def store-relation-queue (async/chan (:store-relation buffers)))

(defn pick-next
  "Choose the next time from the queue, favoring the fast lane"
  [fast-queue slow-queue]
  (first (async/alts!! [fast-queue slow-queue] :priority true)))


(defn pick-next-friend
  "Choose the next node whose friends to look up"
  []
  (pick-next friend-lookup-queue slow-friend-lookup-queue))

(defn pick-next-follower
  "Choose the next node whose followers to look up"
  []
  (pick-next follower-lookup-queue slow-follower-lookup-queue))


(defn lookup-relations-callback
  "Given a node, requeue it in our processing queue once its been stored, looked up in twitter"
  [x]
  (debug "recieved callback ")
  (async/>!!
    lookup-relations-queue
    (wu/remove-data x wu/meta-callback)))


(defn ensure-profile-exists
  "For every input, hand off to create/update node queue, but asked we get processed afterwards"
  [x]
  (debug "make sure profile exists")
  (let [y (wu/merge-metadata x wu/meta-callback lookup-relations-callback)
        data (wu/unit-data y)]
    (if (all-not-nil (:profileId data))
      (un/update-primary-node y)
      (error "missing required data for lookup:" y))))

(defn log-str
  "Convenience function to convert a node into a condensed loggable string"
  [node]
  (str " id:" (:profileId node) " friends:" (:friendCount node) " followers:" (:followerCount node))
  )


(defn propagate
  "For a given node, retrieve its friends/follower counts. If they are bigger than our max-relations,
  stick in the slow lane where we still look up but no as quickly, allowing other dealers to go by"
  [x]
  (info "propagating")
  (let [existing-node (wu/unit-data x)
        timestamp (ctc/to-long (ct/now))
        node-with-time (wu/merge-metadata x wu/meta-timestamp timestamp )]
    (if (and
          (all-not-nil existing-node (:friendCount existing-node) (:followerCount existing-node))
          (= (compare (:friendCount existing-node) max-relations) -1)
          (= (compare (:followerCount existing-node) max-relations) -1))
      (do
        (info "Adding to fast lane..." (log-str existing-node))
        (async/>!! friend-lookup-queue node-with-time)
        (async/>!! follower-lookup-queue node-with-time)
        (sn/store-node (merge (wu/unit-data x) {:processedRelations timestamp})))
      (do
        (info "Adding to slow lane..." (log-str existing-node))
        (async/>!! slow-friend-lookup-queue node-with-time)
        (async/>!! slow-follower-lookup-queue node-with-time)
        (sn/store-node (merge (wu/unit-data x) {:processedRelations timestamp}))))))


(defn- enqueue-relations
  "Create relations based on the data we "
  [type results work]
  ;(debug "enquing relations" type results work)
  (doall (map neo/bulk-update-relationships
              (map
                (fn [x] {:start (:profileId (wu/unit-data work)) :type type :ends x :timestamp (:timestamp work)})
                (partition-all 250 (map to-str (:ids (:body results)))))))
  (if (= "0" (:next_cursor_str (:body results)))
    (neo/delete-relationships {:start (:profileId (wu/unit-data work)) :type type :timestamp (wu/unit-metadata wu/meta-timestamp work)}))
  results)

(defn- enqueue-node [results work]
  (debug "enquing node")
  ;(async/>!! update-node/node-lookup-queue work)
  (doall (map neo/create-or-update-node (map #(hash-map :profileId (to-str %1)) (:ids (:body results)))))
  (map un/ensure-secondary-queue (map #(hash-map :profileId (to-str %1)) (:ids (:body results))) false)
  results)

(defn slow [results work]
  (Thread/sleep 30000)
  results
  )

;; friends/followers are basically the same function, lets define a base
;; function and use partial application to pass our "type"
(def enqueue-friends (partial enqueue-relations "friends"))
(def enqueue-followers (partial enqueue-relations "followers"))

;; Lets define our process chains, then wrap the process chains in
;; in paging logic so that if there are multiple pages of results,
;; the process chain gets repeated. Unfortunately we get 15 lookups every 15 minutes so if
;; we'll need a lot more threads if we are going to handle bit twitter accounts (even at 5k results at a time, a
;; multi-million result will just clog the pipe). We should have the node info - maybe don't get if > x?


(def follower-chain
  (partial inject-timestamp
           (partial page-results
                    (partial process-chain to/lookup-followers [enqueue-node enqueue-followers rate-limit]))))
(def friend-chain
  (partial inject-timestamp
           (partial page-results
                    (partial process-chain to/lookup-friends [enqueue-node enqueue-friends rate-limit]))))


(defn load-unloaded-dealers []
  (load-queue lookup-queue neo/find-unprocessed-nodes))

(defn load-previous-dealers []
  (load-queue lookup-queue neo/find-nodes-not-processed-lately))


(defn load-masses []
  true)


(def data-loop [load-unloaded-dealers load-previous-dealers load-masses])



(defn load-data []
  (async/thread
    (loop [steps data-loop]
      (let [head-fn (first steps)
            tail (rest steps)
            should-still-go (head-fn)]
        (info "Loading relation queue using " head-fn ", good? " should-still-go)
        (if should-still-go
          (if (empty? tail)
            (recur data-loop)
            (recur tail)))))))

(defn start []
  (thread-loop #(async/<!! lookup-queue) ensure-profile-exists)
  (thread-loop #(async/<!! lookup-relations-queue) propagate)
  (thread-loop pick-next-follower follower-chain)
  (thread-loop pick-next-friend friend-chain)
  ;(thread-loop #(async/<!! store-relation-queue) neo/create-or-update-relationship)
  ;(thread-loop #(async/<!! store-relation-queue) neo/bulk-update-relationships)
  [lookup-queue
   lookup-relations-queue store-relation-queue
   friend-lookup-queue slow-friend-lookup-queue
   follower-lookup-queue slow-follower-lookup-queue]
  )
