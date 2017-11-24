(ns fulcro-sql.util
  (:require [clojure.zip :as zip]))

(declare focus-query*)

(defn recursion? [x] (or (= '... x) (number? x)))

(defn join-key [expr]
  (cond
    (map? expr) (let [k (ffirst expr)]
                  (if (list? k)
                    (first k)
                    (ffirst expr)))
    (seq? expr) (join-key (first expr))
    :else expr))

(defn- focused-join [expr ks full-expr union-expr]
  (let [expr-meta (meta expr)
        expr'     (cond
                    (map? expr)
                    (let [join-value (-> expr first second)
                          join-value (if (and (recursion? join-value)
                                           (seq ks))
                                       (if-not (nil? union-expr)
                                         union-expr
                                         full-expr)
                                       join-value)]
                      {(ffirst expr) (focus-query* join-value ks nil)})

                    (seq? expr) (list (focused-join (first expr) ks nil nil) (second expr))
                    :else expr)]
    (cond-> expr'
      (some? expr-meta) (with-meta expr-meta))))

(defn- focus-query*
  [query path union-expr]
  (if (empty? path)
    query
    (let [[k & ks] path]
      (letfn [(match [x]
                (= k (join-key x)))
              (value [x]
                (focused-join x ks query union-expr))]
        (if (map? query)                                    ;; UNION
          {k (focus-query* (get query k) ks query)}
          (into [] (comp (filter match) (map value) (take 1)) query))))))

(defn focus-query
  "Given a query, focus it along the specified path.

  Examples:
    (focus-query [:foo :bar :baz] [:foo])
    => [:foo]

    (fulcro.client.primitives/focus-query [{:foo [:bar :baz]} :woz] [:foo :bar])
    => [{:foo [:bar]}]"
  [query path]
  (focus-query* query path nil))

(defn ident?
  "Returns true if x is an ident."
  [x]
  (and (vector? x)
    (== 2 (count x))
    (keyword? (nth x 0))))

(defn- expr->key
  "Given a query expression return its key."
  [expr]
  (cond
    (keyword? expr) expr
    (map? expr) (ffirst expr)
    (seq? expr) (let [expr' (first expr)]
                  (when (map? expr')
                    (ffirst expr')))
    (ident? expr) (cond-> expr (= '_ (second expr)) first)
    :else
    (throw
      (ex-info (str "Invalid query expr " expr)
        {:type :error/invalid-expression}))))


(defn- query-zip
  "Return a zipper on a query expression."
  [root]
  (zip/zipper
    #(or (vector? %) (map? %) (seq? %))
    seq
    (fn [node children]
      (let [ret (cond
                  (vector? node) (vec children)
                  (map? node) (into {} children)
                  (seq? node) children)]
        (with-meta ret (meta node))))
    root))

(defn- move-to-key
  "Move from the current zipper location to the specified key. loc must be a
   hash map node."
  [loc k]
  (loop [loc (zip/down loc)]
    (let [node (zip/node loc)]
      (if (= k (first node))
        (-> loc zip/down zip/right)
        (recur (zip/right loc))))))

(defn- query-template
  "Given a query and a path into a query return a zipper focused at the location
   specified by the path. This location can be replaced to customize / alter
   the query."
  [query path]
  (letfn [(query-template* [loc path]
            (if (empty? path)
              loc
              (let [node (zip/node loc)]
                (if (vector? node)                          ;; SUBQUERY
                  (recur (zip/down loc) path)
                  (let [[k & ks] path
                        k' (expr->key node)]
                    (if (= k k')
                      (if (or (map? node)
                            (and (seq? node) (map? (first node))))
                        (let [loc'  (move-to-key (cond-> loc (seq? node) zip/down) k)
                              node' (zip/node loc')]
                          (if (map? node')                  ;; UNION
                            (if (seq ks)
                              (recur
                                (zip/replace loc'
                                  (zip/node (move-to-key loc' (first ks))))
                                (next ks))
                              loc')
                            (recur loc' ks)))               ;; JOIN
                        (recur (-> loc zip/down zip/down zip/down zip/right) ks)) ;; CALL
                      (recur (zip/right loc) path)))))))]
    (query-template* (query-zip query) path)))

(letfn [(replace [template new-query]
          (-> template (zip/replace new-query) zip/root))]
  (defn reduce-query-depth
    "Changes a join on key k with depth limit from [:a {:k n}] to [:a {:k (dec n)}]"
    [q k]
    (if-not (empty? (focus-query q [k]))
      (let [pos   (query-template q [k])
            node  (zip/node pos)
            node' (cond-> node (number? node) dec)]
        (replace pos node'))
      q)))

