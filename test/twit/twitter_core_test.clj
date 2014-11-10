(ns twit.twitter-core-test
  (:use clojure.data.xml)
  (:require [twit.twitter-core :as undertest]
            [clojure.test :refer :all]
            ))

(deftest stringify-test
  (is (= "Test" (undertest/stringify [{:name "Test"}] :name)))
  (is (= "one,two" (undertest/stringify [{:foo "one"} {:foo "two"}] :foo))))

(deftest get-oauth-test
  (for [x (range 1 25)]
    (for [y [undertest/friends-ref undertest/followers-ref undertest/profiles-ref]]
      (is (not (nil? (undertest/get-oauth y)))))))