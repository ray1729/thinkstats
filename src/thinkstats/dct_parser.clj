(ns thinkstats.dct-parser
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import java.util.zip.GZIPInputStream))

(def dict-line-rx #"^\s+_column\((\d+)\)\s+(\S+)\s+(\S+)\s+%(\d+)(\S)\s+\"([^\"]+)\"")

(defn parse-dict-line
  [line]
  (let [[_ col type name f-len f-spec descr] (re-find dict-line-rx line)]
    {:col    (dec (Integer/parseInt col))
     :type   type
     :name   name
     :f-len  (Integer/parseInt f-len)
     :f-spec f-spec
     :descr  descr}))

(defn read-dict-defn
  [path]
  (with-open [r (io/reader path)]
    (mapv parse-dict-line (butlast (drop 1 (line-seq r))))))

(defn parse-value
  [type raw-value]
  (when (not (empty? raw-value))
    (case type
      ("str12")          raw-value
      ("byte" "int")     (Long/parseLong raw-value)
      ("float" "double") (Double/parseDouble raw-value))))

(defn make-row-parser
  [dict]
  (fn [row]
    (reduce (fn [accum {:keys [col type name f-len]}]
              (let [raw-value (str/trim (subs row col (+ col f-len)))]
                (assoc accum (keyword name) (parse-value type raw-value))))
            {}
            dict)))

(defn reader
  "Open path with io/reader; coerce to a GZIPInputStream if suffix is .gz"
  [path]
  (if (.endsWith path ".gz")
    (io/reader (GZIPInputStream. (io/input-stream path)))
    (io/reader path)))

(defn read-dct-data
  [dict rdr]
  (let [parse-fn (make-row-parser dict)]
    (map parse-fn (line-seq rdr))))

(comment
  (def dict-path "data/2002FemPreg.dct.txt")
  (def data-path "data/2002FemPreg.dat.gz")

  (def dict (read-dict-defn dict-path))

  (def xs (with-open [r (reader data-path)]
            (doall (read-dct-data dict r))))

  (require '[clojure.pprint :refer [pprint]])

  (pprint
   (take 10 (map
             (fn [x] (select-keys x [:caseid :prglngth :outcome :pregordr
                                     :birthord :birthwgt_lb :birthwgt_oz
                                     :agepreg :finalwgt]))
                        xs)))

  )
