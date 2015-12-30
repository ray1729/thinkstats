(ns thinkstats.gorilla
  (:require [gorilla-renderable.core :as render]
            incanter.core)
  (:import incanter.core.Dataset))

;; Borrowed from gorilla-repl.table
(defn- list-like
  [data value open close separator]
  {:type :list-like
   :open open
   :close close
   :separator separator
   :items data
   :value value})

(extend-type incanter.core.Dataset
  render/Renderable
  (render [this]
    (let [cols    (incanter.core/col-names this)
          heading (list-like (map render/render cols) (pr-str cols) "<tr><th>" "</th></tr>" "</th><th>")
          rows    (map (fn [row]
                         (list-like (map render/render row) (pr-str row) "<tr><td>" "</td></tr>" "</td><td>"))
                       (incanter.core/to-vect this))]
      (list-like (into [heading] rows) (pr-str this) "<center><table>" "</table></center>" "\n"))))
