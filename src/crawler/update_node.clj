(ns crawler.update-node
  (require [clojure.core.async :as async]
           [db.neo4j-core :as neo]
           [clojure.tools.logging :refer (error info debug)]
           [crawler.blocks :refer :all]
           [crawler.store-node :as sn]
           [twit.twitter-core :as to]
           [clj-time.core :as ct]
           [clj-time.coerce :as ctc]
           [util.assertion :refer :all]
           [util.workunit :as wu]
           ))

;; This workflow will take nodes, attempt to see if we have current
;; twitter profile data or if its not fleshed out (like if we stored just the
;; twitter profile id to create a relationship) then retrieve data and merge the
;; data into  neo4j

(def profile-expires (* 1000 60 60 24 7))

(def buffers {:ensure-primary   (async/buffer 1500)
              :ensure-secondary (async/buffer 1500)
              :clean-data       (async/buffer 500)
              :twitter-lookup   (async/buffer 200)})

(def clean-data-queue (async/chan (:clean-data buffers)))
(def ensure-primary-queue (async/chan (:ensure-primary buffers)))
(def ensure-secondary-queue (async/chan (:ensure-secondary buffers)))

(defn pick-next-preexisting []
  (first (async/alts!! [ensure-secondary-queue ensure-primary-queue clean-data-queue] :priority true)))

(def twitter-lookup-queue (async/chan (:twitter-lookup buffers)))

(defn update-primary-node
  "Check to see if we need to update the node with fresh twitter data,
  if so do the needful"
  [node]
  (async/>!! ensure-primary-queue node))

(defn update-secondary-node
  "Given a node we are loading via checking a node's relationships, check
  to see if we have data and if not, do the needful"
  [node]
  (async/>!! ensure-secondary-queue node))


(defn preexisting? [node]
  (let [unwrapped (wu/unit-data node)
        existing-node (:data (neo/lookup-node (:profileId unwrapped)))
        timestamp (ctc/to-long (ct/now))]
    (info "Do I need to lookup" (:profileId unwrapped) " timestamp:" timestamp " existing-node:" (:profileId existing-node))
    (if (or ((complement contains?) existing-node :name)
            ((complement contains?) existing-node :retrieved)
            (= (compare (- timestamp (:retrieved existing-node)) profile-expires) 1))
      (do (info "Need to process " (:profileId unwrapped) "Name:" (:name existing-node) "Timestamp:" (:retrieved existing-node))
          (async/>!! twitter-lookup-queue node))
      (do (debug "No need to process checking for callback..")
          (if (wu/unit-metadata-contains? node wu/meta-callback)
            ((wu/unit-metadata wu/meta-callback node) node))))))


(defn dealer-q-reader [] (read-q twitter-lookup-queue))


(defn enqueue-nodes
  [results nodes]
  (debug "Updating nodes with twitter data")
  (let [nodeMap (#(zipmap (map (fn[x](:profileId (wu/unit-data x))) %) %) nodes)]
    (doall (map sn/store-node
                   (map (fn [x]
                          ;(debug "TwitterUpdateNode: " (get nodeMap (to-str (:id_str x))))
                          (wu/merge-data
                                  (get nodeMap (to-str (:id_str x)))
                                  {:profileId     (to-str (:id_str x))
                                 :profileName   (:screen_name x)
                                 :name          (:name x)
                                 :statusCount   (:statuses_count x)
                                 :followerCount (:followers_count x)
                                 :friendCount   (:friends_count x)
                                 :retrieved     (ctc/to-long (ct/now))
                                 }))
                        (:body results))))
  results))



;; Our processing chain
;; - lookup profile data for a chunk of nodes
;; - process nodes, add to queue for updating
;; - process profiles, add to queue for add/update
(def profile-chain (partial process-chain to/lookup-profiles-id [enqueue-nodes rate-limit]))


(defn load-unretrieved []
  (load-queue clean-data-queue neo/find-nodes-missing-profiles))

(defn load-old-nodes []
  (load-queue clean-data-queue neo/find-nodes-with-old-profiles))

(def data-loop [load-unretrieved load-old-nodes])


(defn load-data []
  (async/thread
    (loop [steps data-loop]
      (let [head-fn (first steps)
            tail (rest steps)
            should-still-go (head-fn)]
        (info "Loading update queue using " head-fn ", good? " should-still-go)
        (if should-still-go
          (if (empty? tail)
            (recur data-loop)
            (recur tail)))))))

(defn start []
  (thread-loop pick-next-preexisting preexisting?)
  (thread-loop #(chunker 100 dealer-q-reader) profile-chain)
  [ensure-primary-queue ensure-secondary-queue twitter-lookup-queue]
  )
