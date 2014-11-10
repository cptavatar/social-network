(ns util.socialite-test
  (:import util.socialite.Socialite)
  (:require [clojure.test :refer :all]
            [util.socialite :as undertest]))


(deftest zero-if-nil-tests
  (let [is-zero-if-nil
        (fn [x fx] (is (= (undertest/zero-if-nil x) fx)))]
    (is-zero-if-nil nil 0)
    (is-zero-if-nil "1" 1)
    (is-zero-if-nil 1 1)
    (is-zero-if-nil "1.1" 1.1)
    (is-zero-if-nil 1.1 1.1)
    ))

(deftest nil-if-empty-tests
  (let [is-nil-if-empty
        (fn [x fx] (is (= (undertest/nil-if-empty x) fx)))]
    (is-nil-if-empty [] nil)
    (is-nil-if-empty ['a'] ['a'])
    ))

(deftest create-socialite-tests
  (is (=
        (Socialite. "webid" "name" "profileId" "profileName" ["brands"]
                    "city" "state" "postal" "country" 1.1 2.1 5 10 15 100 1000)
        (undertest/create-socialite {:webid "webid" :profileId "profileId" :profileName "profileName" :name "name"
                                     :city "city" :brands [ "brands" ] :state "state" :postal "postal" :country "country"
                                     :latitude 1.1 :longitude 2.1 :statusCount 5 :followerCount 10 :friendCount 15
                                     :retrieved 100 :processedRelations 1000
                                     })
        ))

  )

(deftest convert-to-map
  (let [test-map {:webid "webid" :latitude (Float. 1.1) :followerCount (Integer. 10)}]
    (is (= test-map (undertest/convert-to-map (undertest/create-socialite test-map))))
    )
  )


(run-tests)