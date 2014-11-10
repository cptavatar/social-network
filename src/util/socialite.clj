(ns util.socialite)

; A socialiate is simply the data structure we pass around that represents
; a dealer or its follower. We could use records, but we use simple maps - this file contains
; functions to help reduce repeating ourselves.


(defrecord Socialite [^String webid
                      ^String name
                      ^String profileId
                      ^String profileName
                      brands
                      ^String city
                      ^String state
                      ^String postal
                      ^String country
                      ^Float latitude
                      ^Float longitude
                      ^Integer statusCount
                      ^Integer followerCount
                      ^Integer friendCount
                      ^Long retrieved
                      ^Long processedRelations])

(defn zero-if-nil
  "For incoming numeric data, set to 0 if the value
  is nil, if its a string use 'read-string', else return
  (maybe should call this ensure number?)"
  [x]
  (cond
    (nil? x)
    0
    (string? x)
    (read-string x)
    :default
    x))


(defn nil-if-empty
  "Ensure consistency - if we get an empty collection, just store as nil"
  [x]
  (if (or (nil? x) (empty? x))
    nil
    x))

(defn ensure-string
  "Sometimes neo4j returns as values as numerics, ensure they are strings..."
  [x]
  (if (isa? (type x) java.lang.Number)
    (str x)
    x))

(defn create-socialite
  "Default 'constructor' helper for socialite, takes a map and does the needfull..."
  [x]
  (try
  (Socialite. (:webid x)
              (:name x)
              (ensure-string (:profileId x))
              (:profileName x)
              (nil-if-empty(:brands x))
              (:city x)
              (:state x)
              (ensure-string (:postal x))
              (:country x)
              (zero-if-nil (:latitude x))
              (zero-if-nil (:longitude x))
              (zero-if-nil (:statusCount x))
              (zero-if-nil (:followerCount x))
              (zero-if-nil (:friendCount x))
              (zero-if-nil (:retrieved x))
              (zero-if-nil (:processedRelations x)))
  (catch Exception e (str "caught exception creating object: " (.getMessage e) (pr-str x))))
  )

(defn convert-to-map
  "Filter out any null/0 values and condense record into a map"
  [record]
  (into {}
        (filter
          (fn [x]
            (let [v (second x)]
              (and (not= v 0) (not= v 0.0) (not (nil? v)))))
          record)))

(defn create-socialite-legacy
  "Old attribute names, still used in EDN export file (until I update...)"
  [x]
  (Socialite. (:webid x)
              (:name x)
              (:tw-profile-id x)
              (:tw-profile x)
              (nil-if-empty(:brands x))
              (:city x)
              (:state x)
              (ensure-string (:postal x))
              (:country x)
              (zero-if-nil (:lat x))
              (zero-if-nil (:long x))
              (zero-if-nil (:statuses_count x))
              (zero-if-nil (:followers_count x))
              (zero-if-nil (:friends_count x))
              (zero-if-nil (:retrieved x))
              (zero-if-nil (:processed_relations x))))

