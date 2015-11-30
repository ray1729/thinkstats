(ns thinkstats.dct-parser
  ^{:doc "Parser for Stata dictionary and data files."}
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.core.matrix.dataset :refer [dataset]])
  (:import java.util.zip.GZIPInputStream))

(def dict-line-rx #"^\s+_column\((\d+)\)\s+(\S+)\s+(\S+)\s+%(\d+)(\S)\s+\"([^\"]+)\"")

(defn parse-dict-line
  [line]
  (let [[_ col type name f-len f-spec descr] (re-find dict-line-rx line)]
    {:col    (dec (Integer/parseInt col))
     :type   type
     :name   (str/replace name "_" "-")
     :f-len  (Integer/parseInt f-len)
     :f-spec f-spec
     :descr  descr}))

(defn read-dict-defn
  "Read a Stata dictionary file, return a vector of column definitions."
  [path]
  (with-open [r (io/reader path)]
    (mapv parse-dict-line (butlast (rest (line-seq r))))))

(defn parse-value
  [type raw-value]
  (when (not (empty? raw-value))
    (case type
      ("str12")          raw-value
      ("byte" "int")     (Long/parseLong raw-value)
      ("float" "double") (Double/parseDouble raw-value))))

(defn make-row-parser
  "Parse a row from a Stata data file according to the specification in `dict`.
   Return a vector of columns."
  [dict]
  (fn [row]
    (reduce (fn [accum {:keys [col type name f-len]}]
              (let [raw-value (str/trim (subs row col (+ col f-len)))]
                (conj accum (parse-value type raw-value))))
            []
            dict)))

(defn reader
  "Open path with io/reader; coerce to a GZIPInputStream if suffix is .gz"
  [path]
  (if (.endsWith path ".gz")
    (io/reader (GZIPInputStream. (io/input-stream path)))
    (io/reader path)))

(defn read-dct-data
  "Parse lines from `rdr` according to the specification in `dict`.
   Return a lazy sequence of parsed rows."
  [dict rdr]
  (let [parse-fn (make-row-parser dict)]
    (map parse-fn (line-seq rdr))))

;; This function isn't really needed as we could create a dataset
;; and use clojure.core.matrix.dataset/row-maps
(defn as-maps
  "Read Stata data set, return a vector of maps."
  [dict-path data-path]
  (let [dict     (read-dict-defn dict-path)
        col-keys (map (comp keyword :name) dict)]
    (with-open [r (reader data-path)]
      (mapv (fn [cols] (zipmap col-keys cols))
            (read-dct-data dict r)))))

(defn as-dataset
  "Read Stata data set, return a clojure.core.matrix.dataset."
  [dict-path data-path]
  (let [dict   (read-dict-defn dict-path)
        header (map (comp keyword :name) dict)]
    (with-open [r (reader data-path)]
      (dataset header (doall (read-dct-data dict r))))))
