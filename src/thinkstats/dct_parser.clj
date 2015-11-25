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

(defmulti parse-value (fn [type raw-value] type))

(defmethod parse-value :default [_ v] v)

(defn make-row-parser
  [dict]
  (fn [row]
    (reduce (fn [accum {:keys [col type name f-len]}]
              (let [raw-value (not-empty (str/trim (subs row col (+ col f-len))))]
                (assoc accum (keyword name) (parse-value type raw-value))))
            {}
            dict)))

(defn read-dct-data
  [dict-path data-path]
  (let [dict (read-dict-defn dict-path)
        parse-fn (make-row-parser dict)]
    (with-open [in (io/reader (GZIPInputStream. (io/input-stream data-path)))]
      (doall (take 10 (map parse-fn (line-seq in)))))))

(comment
  (def dict-path "data/2002FemPreg.dct.txt")
  (def data-path "data/2002FemPreg.dat.gz")

  (read-dct-data dict-path data-path)

  )
