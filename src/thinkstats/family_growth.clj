(ns thinkstats.family-growth
  ^{:doc "Exploring the National Survey of Family Growth data"}
  (:require [thinkstats.dct-parser :as dct]
            [incanter.core :as i :refer [$]]))

(def dict-path "Thinkstats2/code/2002FemPreg.dct")
(def data-path "Thinkstats2/code/2002FemPreg.dat.gz")

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
    (i/transform-col ds :agepreg centiyears->years)
    (i/transform-col ds :birthwgt-lb clean-birth-weight)
    (i/transform-col ds :birthwgt-oz clean-birth-weight)
    (i/add-derived-column :totalwgt-lb [:birthwgt-lb :birthwgt-oz]
                          compute-totalwgt ds)))

(defn ds-frequencies
  [col-name ds]
  (i/dataset [col-name :frequency] (sort-by key (frequencies ($ col-name ds)))))

;; Build an index of caseid to rows
(defn build-caseid-ix
  [ds]
  (reduce (fn [accum [row-ix caseid]]
            (clojure.core/update accum caseid (fnil conj []) row-ix))
          {}
          (map-indexed vector ($ :caseid ds))))

(def $not-nil {:$fn (complement nil?)})

(defn ensure-collection
  "Wrap `x` in a vector if it is not already a collection."
  [x]
  (if (coll? x) x (vector x)))

(defn sel-defined
  "Variant of Incater's `sel` function that returns only the rows
   where none of the selected columns are nil."
  [ds & {:keys [rows cols]}]
  (let [rows (or rows :all)
        cols (or cols (i/col-names ds))]
    (i/sel ($where (zipmap (ensure-collection cols) (repeat $not-nil))
                   ds)
           :rows rows :cols cols)))

(defn set-invalid-nil
  [ds col valid?]
  (i/transform-col ds col (fn [v] (when (and v (valid? v)) v))))

(comment

  (def ds (dct/as-dataset dict-path data-path))

  ;; Show number of rows and columns in the dataset
  (i/dim ds)

  ;; Show the names of the columns in the dataset
  (i/col-names ds)

  ;; Select the :pregordr column
  ($ :pregordr ds)

  ;; Clean the data
  (def ds' (clean-fem-preg ds))

  ;; Outcome frequencies
  (frequencies ($ :outcome ds'))

  ;; ...as a dataset with rows sorted on outcome
  (def outcomes (ds-frequencies :outcome ds'))

  (view outcomes)

  (clojure.pprint/print-table (:column-names outcomes) (:rows outcomes))

  ;; Top 10 birth weights
  (take 10 (sort > (filter identity ($ :birthwgt-oz ds'))))

  ;; Look at frequencies of birthwgt-lb
  (view (ds-frequencies :birthwgt-lb ds'))

  (def preg-map (build-caseid-ix ds'))

  (preg-map "10229")

  (sel ds' :rows (preg-map "10229") :cols [:outcome])

  ($ (preg-map "10229") [:outcome :agepreg] ds')

  (with-data ds'
    ($ (preg-map "10229") [:outcome :agepreg]))

  (def ds'
    (-> ds
        (set-invalid-nil :birthwgt-lb (complement #{51 97 98 99}))
        (set-invalid-nil :birthwgt-oz (fn [v] (<= 0 v 15)))))


  ;; Chapter 1 exercises

  ;; Read and clean the dataset
  (def ds (clean-fem-preg (dct/as-dataset dict-path data-path)))

  ;; Print frequencies of birth order
  (ds-frequencies :birthord ds)

  ;; Print frequencies of pregnancy length
  (ds-frequencies :prglngth ds)

  ;; Print frequencies of age at pregnancy
  (ds-frequencies :agepreg ds)

  ;; Compute the mean birth weight
  (require '[incanter.stats :as s])
  (s/mean (remove nil? ($ :totalwgt-lb ds)))

  ;; Add new column, total weight it kg
  (defn lb->kg [lb] (when lb (* lb 0.454)))
  (def ds' (i/add-derived-column :totalwgt-kg
                                 [:totalwgt-lb]
                                 lb->kg
                                 ds))

  (s/mean (remove nil? ($ :totalwgt-kg ds')))

  (s/median (remove nil? ($ :totalwgt-kg ds')))

  (def ds'' (i/dataset [:birthwgt] (map vector (remove nil? ($ :totalwgt-kg ds')))))
  (s/summary ds'')

  ;; Generate a sequence of booleans, true when :outcome is 1
  (map (fn [x] (= x 1)) ($ :outcome ds'))

  ;; Create dataset of live births (outcome == 1)
  (def live (i/$where {:outcome 1} ds'))
  (i/dim live)


  (i/$where (i/$fn [totalwgt-kg] (not (nil? totalwgt-kg))) ds')

  (defn summarize-col
    [col ds]
    (s/summary ($ [col] (i/$where (fn [r] (not (nil? (r col)))) ds))))
  )
