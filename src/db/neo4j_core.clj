(ns db.neo4j-core
  (:require [clojurewerkz.neocons.rest :as nr]
            [clojurewerkz.neocons.rest.nodes :as nn]
            [clojurewerkz.neocons.rest.relationships :as nrl]
            [clojurewerkz.neocons.rest.cypher :as cy]
            [clojurewerkz.neocons.rest.index :as ci]
            [clojurewerkz.neocons.rest.labels :as nl]
            [clojure.tools.logging :refer (debug info error)]
            [metrics.timers :as mt]
            [util.assertion :refer :all]
            [util.workunit :as wu]
            [util.socialite :as so]
            [clojure.core.cache :as cache])
  )

; this module specializes in reading/storing data in Neo4J


(mt/deftimer neo-create-node-time)
(mt/deftimer neo-update-node-time)
(mt/deftimer neo-find-node-time)
(mt/deftimer neo-create-relation-time)
(mt/deftimer neo-find-unprocessed-time)
(mt/deftimer neo-find-old-relations-time)
(mt/deftimer neo-find-missing-profiles)
(mt/deftimer neo-find-old-profiles-time)
(mt/deftimer neo-delete-relations-time)

(def con (atom nil))

(def node-index "node_auto_index")
(def rel-index)
(def node-label "_Socialite")

(def profile-id-cache (ref (cache/lru-cache-factory {} :threshold 100000)))

(defn add-node-to-cache
  [node]
  (debug "Adding node to cache: " node)
  (let [profileId (:profileId (:data node))]
    (dosync
      (if (cache/has? @profile-id-cache profileId)
        (alter profile-id-cache cache/hit profileId)
        (alter profile-id-cache cache/miss profileId node)))))

(defn- create-index
  "Try creating a index 'name' on node-label, catching any exeception about it already existing"
  [name]
  (try
    (ci/create @con node-label name)
    (str "Created index " name)
    (catch Exception e (str "Couldn't create index " name ", already exists"))))

(defn neo-init
  "Given server/port, initialize the neo4j subystem"
  [server port]
  (swap! con (fn [x] (nr/connect (str "http://" server ":" port "/db/data/"))))
  (map create-index ["profileId" "profileName" "webid"]))

(defn ret-all-indexes
  "Convenience function to list all the known indexes using the current live connection"
  []
  (nn/all-indexes @con))


(defn create-node
  "Given a record, create a new node in the DB. Default to type 'user' if not provided,
  make sure we add appropritate field for SpringData... "
  [map]
  (let [unit (wu/make-unit map)
        node (mt/time! neo-create-node-time (nn/create @con (merge
                                                     {:type "user"}
                                                     (so/convert-to-map (wu/unit-data unit))
                                                     )))]
    (add-node-to-cache node)
    (nl/add @con node "_Socialite")
    node
    ))


(defn update-node
  "Given a node with an id, update. Make sure to strip out any empty values from our record first"
  [node]
  (debug "update node " (pr-str node))
  (mt/time! neo-update-node-time
            (nn/update @con
                       (:id node)
                       (so/convert-to-map (wu/unit-data node))))
  node)

(defn find-nodes
  "For a provided key/value try to find all nodes that match. Will use the auto-index"
  [k v]
  (try
    (mt/time! neo-find-node-time (nn/find @con node-index k v))
    (catch Exception e nil)))

(defn lookup-node
  "Given a profile id, see if we have it in the cache - if we do, serve it up. If not,
  go ahead and look it up"
  [profile-id]
  (if (cache/has? @profile-id-cache profile-id)
    (do
      (info "Cache hit for " profile-id)
      (cache/lookup @profile-id-cache profile-id))
    (let [node (first (find-nodes "profileId" profile-id))]
      (debug "Miss - node value" node)
      (if (not-nil? node)
        (do
          (info "Cache miss for" profile-id ", but found in db " node)
          (dosync
            (alter profile-id-cache cache/miss profile-id node))
          node)
        (do
          (info "Cache miss for" profile-id ", nothing found - going to need to retrieve from twitter still")
          nil)))))

(defn find-node-relations
  "For a given node and relationship type, find all relationships"
  [node type]
  (nrl/all-for @con node {:keys [type]}))

(defn create-or-update-node
  ([input]
   (let [node (wu/make-unit input)]
     (cond
       (contains? node :id)
       (update-node node)
       (contains? (wu/unit-data node) :profileId)
       (create-or-update-node node "profileId")
       :default
       (create-or-update-node node "webid")
       )))
  ([node key]
   (if-let [existing (first (find-nodes key ((keyword key) (:data node))))]
     (update-node (merge existing {:data (merge (:data existing) (:data node))}))
     (create-node node))))

(defn find-relationship [start end type]
  (debug "Finding relationship" start end type)
  (nrl/first-outgoing-between @con start end [type]))


(defn create-relationship
  [start end type data]
  (debug "creating relationship" start end type)
  (nrl/create @con start end type data))


(defn update-relationship
  [rel data]
  (debug "update relationship" rel data)
  (nrl/update @con rel data))

(defn create-or-update-relationship
  ([data]
   (debug "Create or update relationship" data)
   (create-or-update-relationship (:start data) (create-or-update-node {:profileId (:end data)}) (keyword (:type data)) {:timestamp (:timestamp data)}))
  ([start end type data]
   (if-let [existing (find-relationship start end type)]
     (update-relationship existing data)
     (create-relationship start end type data))))


(defn neo-reset-data []
  "WARNING! This will drop your data!"
  (cy/tquery @con "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r"))


;; By default, neo4j gives us a ton of data, limit it to the properties
;; we care about. Downside? We have to maintain this list....
(def return-props
  ["webid", "longitude",
   "name", "profileId",
   "city", "type",
   "state", "statusCount",
   "country", "postal",
   "profileName", "retrieved",
   "friendCount",
   "latitude", "followerCount",
   "brands", "processedRelations"])

(def return-clause (str "RETURN "
                        (reduce str
                                (map (fn [x] (str "n.`" x "`, "))
                                     return-props))))
(def return-size "limit 200")

(defn build-node-result
  [result]
  (so/create-socialite (reduce merge (map (fn [x y] (assoc {} (keyword x) y)) return-props result))))

(defn convert-response [response]
  (map build-node-result (:data response)))

(defn node-search [query timer]
  (mt/time! timer (convert-response (cy/query @con query))))

(defn find-unprocessed-nodes
  "Return dealer profiles that we have not processed at all yet"
  []
  (node-search (str "MATCH n where n.type = 'dealer' and not HAS(n.`processedRelations`) " return-clause " id(n) " return-size)
               neo-find-unprocessed-time))

(defn find-nodes-not-processed-lately
  []
  (node-search (str "MATCH n where n.type = 'dealer' " return-clause " id(n) order by n.`processedRelations` asc  " return-size)
               neo-find-old-relations-time))


(defn find-nodes-missing-profiles
  "Return dealer profiles that we have not processed at all yet"
  []
  (node-search (str "MATCH n where n.type = 'dealer' and not HAS(n.`processed`) " return-clause " id(n) " return-size)
               neo-find-missing-profiles))


(defn find-nodes-with-old-profiles
  []
  (node-search (str "MATCH n where n.type = 'dealer' " return-clause " id(n) order by n.`processed` asc " return-size)
               neo-find-old-profiles-time))


(defn find-all-relationships []
  (map #(hash-map :from (get %1 "a.`profileId`") :type (get %1 "type(r)") :timestamp (get %1 "r.timestamp") :to (get %1 "b.`profileId`"))
       (cy/tquery @con "MATCH (a {type: 'dealer'})-[r]-(b) RETURN a.`profileId`, type(r), r.timestamp, b.`profileId` ORDER BY r.timestamp "))
  )

(defn delete-relationships [{start :start type :type timestamp :timestamp}]
  (let [query (str "MATCH (a {`profileId`:\"" start "\"})-[r:" type "]->() WHERE r.timestamp <> " timestamp " DELETE r ")]
    (info "Running query " query)
    (mt/time! neo-delete-relations-time (cy/tquery @con query))))

(defn bulk-update-relationships-old [{start :start type :type ends :ends timestamp :timestamp}]
  {:pre [(all-not-nil start type ends timestamp)]}
  (let [query (str "MATCH (a {`profileId`:\"" start "\"}),(b) WHERE b.`profileId` IN ["
                   (clojure.string/join "," (map (fn [x] (str "\"" x "\"")) ends)) "] CREATE UNIQUE a-[r:" type "]->b SET r.timestamp = " timestamp)]
    (info "Running query " query)
    (mt/time! neo-create-relation-time (cy/tquery @con query))))

(defn bulk-update-relationships [{start :start type :type ends :ends timestamp :timestamp}]
  {:pre [(all-not-nil start type ends timestamp)]}
  (info "Creating relations between" start "and" (count ends) "nodes")
  (let [start-node (lookup-node start) end-nodes (doall (map lookup-node ends))]
    (debug "update relationship start node" start-node)
    (debug "update relationship end nodes" end-nodes)
    (mt/time! neo-create-relation-time (doall (nrl/create-many @con start-node end-nodes type {:timestamp timestamp})))))
