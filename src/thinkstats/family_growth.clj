(ns thinkstats.family-growth
  ^{:doc "Exploring the National Survey of Family Growth data"}
  (:require [thinkstats.dct-parser :as dct]
            [clojure.core.matrix :as m]
            [clojure.core.matrix.dataset :as ds]))

(def dict-path "data/2002FemPreg.dct")
(def data-path "data/2002FemPreg.dat.gz")

(def birth-weight-special-value
  {97 "Not ascertained"
   98 "Refused"
   99 "Don't know"})

(defn clean-birth-weight
  "Replace special values for birth weight with nil."
  [row col-name]
  (update row col-name (fn [v] (if (birth-weight-special-value v) nil v))))

(defn centiyears->years
  "Convert centiyears to years."
  [row col-name]
  (update row col-name (fn [v] (when v (/ v 100.0)))))

(defn add-total-weight
  [{:keys [birthwgt-lb birthwgt-oz] :as row}]
  (assoc row
    :totalwgt-lb
    (when (and birthwgt-lb birthwgt-oz)
      (+ birthwgt-lb (/ birthwgt-oz 16.0)))))

(defn clean-fem-preg-row
  [row]
  (-> row
      (centiyears->years :agepreg)
      (clean-birth-weight :birthwgt-lb)
      (clean-birth-weight :birthwgt-oz)
      (add-total-weight)))

(defn clean-fem-preg
  [ds]
  (ds/dataset (map clean-fem-preg-row (ds/row-maps ds))))

(comment

  (def ds (dct/as-dataset dict-path data-path))

  ;; Show number of rows and columns in the dataset
  (count (m/rows ds))
  (count (m/columns ds))

  ;; Show the names of the columns in the dataset
  (ds/column-names ds)

  ;; Create a small dataset for experimenting with
  (def ds' (ds/select-rows ds (range 10)))

  ;; Select the :pregordr column
  (ds/select-columns ds' [:pregordr])

  ;; Apply a function to a column, return a new dataset
  (ds/emap-column ds' :agepreg (fn [v] (when v (/ v 100.0))))


  )
