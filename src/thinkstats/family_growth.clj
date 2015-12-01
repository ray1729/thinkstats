(ns thinkstats.family-growth
  ^{:doc "Exploring the National Survey of Family Growth data"}
  (:require [thinkstats.dct-parser :as dct]
            [incanter.core :refer :all])
  (:refer-clojure :exclude [update]))

(def dict-path "data/2002FemPreg.dct")
(def data-path "data/2002FemPreg.dat.gz")

(def birth-weight-special-value
  {97 "Not ascertained"
   98 "Refused"
   99 "Don't know"})

(defn clean-birth-weight
  "Replace special values for birth weight with nil."
  [v]
  ;;(if (birth-weight-special-value v) nil v)
  (if (and v (> v 15)) nil v))

(defn centiyears->years
  "Convert centiyears to years."
  [v]
  (when v (/ v 100.0)))

(defn compute-totalwgt
  "Comupte the total weight in lb."
  [lb oz]
  (when (and lb oz) (+ lb (/ oz 16.0))))

(defn clean-fem-preg
  [ds]
  (as-> ds ds
    (transform-col ds :agepreg centiyears->years)
    (transform-col ds :birthwgt-lb clean-birth-weight)
    (transform-col ds :birthwgt-oz clean-birth-weight)
    (add-derived-column :totalwgt-oz [:birthwgt-lb :birthwgt-oz]
                        compute-totalwgt ds)))

(comment

  (def ds (dct/as-dataset dict-path data-path))

  ;; Show number of rows and columns in the dataset
  (dim ds)

  ;; Show the names of the columns in the dataset
  (ds/col-names ds)

  ;; Select the :pregordr column
  ($ :pregordr ds)

  ;; Clean the data
  (def ds' (clean-fem-preg ds))

  ;; Outcome frequencies
  (frequencies ($ :outcome ds'))

  ;; ...as a dataset with rows sorted on outcome
  (def outcomes
    (dataset
     [:outcome :frequency]
     (sort-by key
              (frequencies ($ :outcome ds')))))

  (view outcomes)

  (clojure.pprint/print-table (:column-names outcomes) (:rows outcomes))

  ;; Top 10 birth weights
  (take 10 (sort > (filter identity ($ :birthwgt-oz ds'))))

  ;; Look at frequencies of birthwgt-lb
  (view (dataset [:birthwgt-lb :frequency]
                 (sort-by key (frequencies (remove nil? ($ :birthwgt-lb ds'))))))

  ;; Build an index of caseid to rows
  (defn build-caseid-ix
    [ds]
    (reduce (fn [accum [row-ix caseid]]
              (clojure.core/update accum caseid (fnil conj []) row-ix))
            {}
            (map-indexed vector ($ :caseid ds))))

  (def preg-map (build-caseid-ix ds'))

  (preg-map "10229")

  (sel ds' :rows (preg-map "10229") :cols [:outcome])

  ($ (preg-map "10229") :outcome ds')

  )
