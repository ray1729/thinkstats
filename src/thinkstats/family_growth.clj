(ns thinkstats.family-growth
  ^{:doc "Exploring the National Survey of Family Growth data"}
  (:require [thinkstats.dct-parser :as dct]
            [thinkstats.incanter :as ie :refer [$!]]
            [incanter.core :as i :refer [$]]))

(def dict-path "Thinkstats2/code/2002FemPreg.dct")
(def data-path "Thinkstats2/code/2002FemPreg.dat.gz")

(defn compute-totalwgt-lb
  [lb oz]
  (when lb (+ lb (/ (or oz 0) 16.0))))

(defn centiyears->years
  [v]
  (when v (/ v 100.0)))

(defn clean-and-augment-fem-preg
  [ds]
  (as-> ds ds
    (ie/set-invalid-nil ds :birthwgt-lb (complement #{51 97 98 99}))
    (ie/set-invalid-nil ds :birthwgt-oz (fn [v] (<= 0 v 15)))
    (i/transform-col ds :agepreg centiyears->years)
    (i/add-derived-column :totalwgt-lb
                          [:birthwgt-lb :birthwgt-oz]
                          compute-totalwgt-lb
                          ds)))

(defn fem-preg-ds
  ([]
   (fem-preg-ds dict-path data-path))
  ([dict-path data-path]
   (clean-and-augment-fem-preg (dct/as-dataset dict-path data-path))))
