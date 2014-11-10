(ns util.workunit)

(def meta-data :data)
(def meta-callback :callback)
(def meta-timestamp :timestamp)
(def meta-twitter :twitter)

(defn make-unit
  "Attempt to create a map with the node as :data element - if given a record already, return record"
  [node]
  (if (contains? node :data)
    node
    (hash-map :data node)))

(defn unit-data
  "Given what could be a node record, try to return the data element"
  [node]
  (if (contains? node :data)
    (:data node)
    node))

(defn unit-metadata-contains?
  [record key]
  (contains? record key))

(defn unit-metadata
  [key record]
  (key record))

(defn merge-metadata [record key value]
  (merge record {key value}))

(defn merge-data [record data]
  (merge record {:data data}))

(defn remove-data [record key]
  (dissoc record key))