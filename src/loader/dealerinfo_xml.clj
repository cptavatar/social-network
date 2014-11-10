(ns loader.dealerinfo-xml
  (:require
    [clojure.data.xml :refer :all]
    [clojure.tools.logging :refer (debug)]
    [clojure.java.io :refer :all]))

;;;; Functionality around parsing our dealer XML file
;;;; and making it available

;;; Our xml file - this is a lazy sequence that is loaded as read
(def xml-doc (xml-seq (parse (input-stream (file "dealerInfo.xml")))))

;;; Helper to walk a sequence, applying a function
;;; to each entry, return a default collection, and merging using collection-appropriate,
;;; user defined merge function
(defn walk-seq
  [f def-val merge-f seq-val]
  (cond
    (seq? seq-val)
    (let [[head & rest] seq-val]
      (merge-f (f head) (walk-seq f def-val merge-f rest)))
    (empty? seq-val)
    def-val
    :default (f seq-val)))

;;; Multimethod - route which process-tag dispatch by the tag that gets
;;; returned.
(defmulti process-tag #(:tag %1))

;;; The main node processing function. Walk the sequence, merging the results of
;;; processing nodes into a single map
(defn process-node [x]
  (walk-seq process-tag {} merge x))


(defn print-item [x]
  (println (element :tag x)) x)

;;; Delegate for address elements
(defmethod process-tag :addr [addr-node]
  (let
      [atts (:attrs addr-node)]
    {:city  (:city atts)
     :state (:state atts)
     :zip   (:zip atts)
     :cntry (:cntry atts)
     :lat   (:lat atts)
     :long  (:long atts)}))

;;; Delegate for site elements
(defmethod process-tag :site [site-node]
  (let
      [atts (:attrs site-node)]
    (merge {:name  (:name atts)
            :webid (:webid atts)}
           (process-node (:content site-node)))))

;;; Walk the list of sites
(defn process-sites
  [node operation]
  (doseq
      [child (:content node)]
    (operation (process-node child))))

;;; Process individual brand elements
(defn process-brand
  [x]
  (walk-seq #(list (first (:content %1))) '() concat x))

;;; Process the outer <brands> element
(defmethod process-tag :brands [node]
  {:brands (process-brand (:content node))})

;;; Clean up some of the key names a bit
(defn remap-keys
  [x]
  (cond
    (= x "account_twitterUser")
    "profileName"
    (= x "facebook_profileId")
    "fb-profile-id"
    (= x "facebook_pageName")
    "fb-page-name"
    (= x "socialMediaSites_googlePlacesURL")
    "goog-places-url"
    :default
    "_"
    ))

;;; Some of the twitter accounts start with http://, lets replace
(defn strip-http [x]
  (let [t (clojure.string/trim x) y (re-find #"\w+$" t)]
    (debug "Cleaning twitter profile:" x "->" y)
    y))

(defn process-acct-value [x]
  (let [v (:value (:attrs x))]
    (if (= (:name (:attrs x)) "account_twitterUser")
      (strip-http v)
      v)))

(defn process-acct
  [x]
  (walk-seq #(hash-map
              (keyword (remap-keys (:name (:attrs %1))))
              (process-acct-value %1)) {} merge x))

;;; Process the outer account element
(defmethod process-tag :accts [node]
  (dissoc (process-acct (:content node)) :_))

(defn parse-stream
  [doc operation]
  (process-sites (first doc) operation))
