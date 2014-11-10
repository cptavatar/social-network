(ns util.assertion
  )

;; Utility functions to make assertions easier

(def not-nil? (complement nil?))

(defn all-not-nil
  "Convenience function to check if all items are not-nil "
  ([x]
   (not-nil? x))
  ([x & rest]
   (if (nil? x)
     false
     (reduce (fn [x y] (and x y))
             (map not-nil? rest)))))
