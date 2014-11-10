(ns loader.load-data
  (require [clojure.core.async :as async]
           [util.assertion :refer :all]
           [twit.twitter-core :as to]
           [clojure.tools.logging :refer (debug info error)]
           [crawler.blocks :refer :all]
           [crawler.store-node :as sn]
           [clj-time.core :as ct]
           [clj-time.coerce :as ctc]
           ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; This workflow is for loading data
;; from an EDN file to initialize the system
;; (after dropping data)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn valid-profile?
  "Perform minor validation to verify incoming data is worth storing"
  [x]
  (not-nil? (:profileName x)))




(defn enqueue-nodes
  "Given a collection of nodes to look up twitter profiles for
  and the results of the twitter request to do so,
  augment the nodes we found data for and stick on a queue
  so that it can be updated."
  [results nodes]
  (let [nodes-with-keys (into {} (map (fn [x] [(keyword (clojure.string/trim (clojure.string/lower-case (:profileName x)))) x]) nodes))]
    (doall (map sn/store-node
                     (map (fn [x] (merge
                                    ((keyword (clojure.string/lower-case (:screen_name x))) nodes-with-keys)
                                    {:profileId       (to-str (:id_str x))
                                     :profileName     (:screen_name x)
                                     :statusCount     (:statuses_count x)
                                     :friendCount     (:friends_count x)
                                     :followerCount   (:followers_count x)
                                     :type            "dealer"
                                     :retrieved       (ctc/to-long (ct/now))
                                     }))
                          (:body results)))))
  results)


;; Our processing chain
;; - lookup profile data for a chunk of nodes
;; - process nodes, add to queue for updating
(def profile-chain (partial process-chain to/lookup-profiles-name [enqueue-nodes rate-limit]))

(defn start [collection]
  (let [source-queue (async/chan)
        dealer-q-reader (fn [] (read-q source-queue))
        store-queues (sn/start)]

    (thread-loop #(chunker 100 dealer-q-reader) profile-chain)
    (async/onto-chan source-queue (filter valid-profile? collection))
    (shutdown-watch store-queues)
    )
  )
