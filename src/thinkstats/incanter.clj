(ns thinkstats.incanter
  ^{:doc "Helper functions for working with Incanter"}
  (:require [incanter.core :as i :refer [$]]))

(def $not-nil
  "Predicate function for use in `$where`."
  {:$fn (complement nil?)})

(defn ensure-collection
  "If `x` is not a collecton, wrap in a vector."
  [x]
  (if (coll? x) x (vector x)))

(defn sel-defined
  "A variant on `sel` that returns only the rows where all of the
  selected columns are defined."
  [ds & {:keys [rows cols]}]
  (let [rows (or rows :all)
        cols (or cols (i/col-names ds))]
    (i/sel (i/$where (zipmap (ensure-collection cols) (repeat $not-nil))
                     ds)
           :rows rows :cols cols)))

(defn $!
  "A variant on `$` that returns only the rows where all of the selected
  columns are defined. Like `$`, this may use the dataset bound to
  `incanter.core/$data` if the dataset argument is omitted.

  ($! cols) => (sel-distinct $data :cols cols)

  ($! rows cols) => (sel-distinct $data :rows rows :cols cols)

  ($! rows nil) => (sel-distinct $data :rows rows)

  ($! cols dataset) => (sel-distinct dataset :cols cols)

  ($! rows cols dataset) => (sel-distinct dataset :rows rows :cols cols)

  ($! rows nil dataset) => (sel-distinct dataset :rows rows)"
  ([cols]
     (sel-defined i/$data :cols cols))

  ([arg1 arg2]
     (cond
      (or (i/matrix? arg2) (i/dataset? arg2))
      (sel-defined arg2 :cols arg1)

      (nil? arg2)
      (sel-defined i/$data :rows arg1)

      :else
      (sel-defined i/$data :rows arg1 :cols arg2)))

  ([rows cols ds]
     (sel-defined ds :rows rows :cols cols)))


(defn set-invalid-nil
  "Return a new dataset where the values of `col` that do not satisfy
  the predicate `valid?` are set to `nil`."
  [ds col valid?]
  (i/transform-col ds col (fn [v] (when (and (not (nil? v)) (valid? v)) v))))

(defn build-column-ix
  "Build an index from `col-name` value to row number."
  [col-name ds]
  (reduce (fn [accum [row-ix v]]
            (update accum v (fnil conj []) row-ix))
          {}
          (map-indexed vector ($ col-name ds))))
