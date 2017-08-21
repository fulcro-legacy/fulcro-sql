(ns fulcro-sql.core
  (:require [clojure.string :as str]))

(defn sqlize [kw]
  (let [nspc (some-> kw namespace (str/replace "-" "_"))
        nm   (some-> kw name (str/replace "-" "_"))]
    (if nspc
      (keyword nspc nm)
      (keyword nm))))

(defn table-for [schema query]
  (let [{:keys [om->sql]} schema
        nses (reduce (fn
                       ([] #{})
                       ([s p]
                        (let [sql-prop (get om->sql p p)
                              table-kw (some-> sql-prop namespace keyword)]
                          (cond
                            (= :db/id sql-prop) s
                            (and sql-prop table-kw) (conj s table-kw)
                            :else s)))) #{} query)]
    (assert (= 1 (count nses)) (str "Could not determine a single table from the subquery " query))
    (sqlize (first nses))))

(defn column-spec [table-alias schema omprop]
  (let [{:keys [om->sql]} schema
        sqlprop (get om->sql omprop omprop)
        column   (name sqlprop) ]
    (str table-alias "." column " AS \"" omprop "\"")))
