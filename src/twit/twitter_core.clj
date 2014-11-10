(ns twit.twitter-core
  (:require
    [twitter.oauth :refer :all]
    [twitter.callbacks :refer :all]
    [twitter.callbacks.handlers :refer :all]
    [twitter.api.restful :refer :all]
    [twit.twitter-oauth :as toa]
    [clojure.tools.logging :refer (debug info error)]
    [metrics.timers :as mt]
    [util.workunit :as wu]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Twitter core operations
;; - my oauth information is stored in twitter_oauth, please use your own.
;; - we used twit.twitter_* to distinguish between our code, what others have written
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;




; Maintain the list of tokens for each operation independently since
; the rate limits vary by operation.
(def friends-ref (ref toa/all-tokens))
(def followers-ref (ref toa/all-tokens))
(def profiles-ref (ref toa/all-tokens))

(defn get-oauth
  "Given a reference, return the next oauth token. If take the last one, reset the reference to the full list of tokens"
  [ref-name]
  (dosync
    (let [token (first @ref-name) tail (alter ref-name rest)]
      (if (empty? tail)
        (alter ref-name concat toa/all-tokens))
      token)))

; Timers to measure how often, how long operations took
(mt/deftimer tw-followers-ids-time)
(mt/deftimer tw-friends-ids-time)
(mt/deftimer tw-profile-time)

(defn stringify
  "Condense a list of dealer profiles into a single comma delimited string of twitter screen names
   Takes a function to extract the data to pull the data out of the profile"
  [profiles map-key]
  (clojure.string/join ","
                       (filter
                         #((complement nil?) %1) (map #(map-key (wu/unit-data %1)) profiles))))

(defn lookup-profiles
  "Lookup user profile in blocks up of up to 100 by screen name"
  [map-key tw-key input]
  (let [names (stringify input map-key)]
    (debug "Looking up twitter profiles for" names)
    (mt/time! tw-profile-time (users-lookup :oauth-creds (get-oauth profiles-ref) :params {tw-key names}))))

; Partial functions used to look up profiles either by name or by id
; We only should need by-name when seeding from external sources
(def lookup-profiles-name (partial lookup-profiles :profileName :screen-name))
(def lookup-profiles-id (partial lookup-profiles :profileId :user-id))


(defn cursorable
  "Generic function to handle looking up the current cursor's data, where
   retriever is the function to look up the data, ref-name is the reference used to get access to the oauth token,
   timer is the timer used to record how long this operation took, and input is the arguments to the lookup"
  [retriever ref-name timer input]
  (let [id (:profileId (:data input))
         cursor (if (contains? input :cursor) (:cursor input) -1)]
     (info "Looking up twitter data for cursor" cursor)
     (mt/time! timer (retriever :oauth-creds (get-oauth ref-name) :params {:user-id id :cursor cursor}))))

; Partial function definitions for lookup either friends (who you follow) or followers (who follows you)
(def lookup-followers (partial cursorable followers-ids followers-ref tw-followers-ids-time))
(def lookup-friends (partial cursorable friends-ids friends-ref tw-friends-ids-time))





