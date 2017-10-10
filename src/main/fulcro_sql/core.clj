(ns fulcro-sql.core
  (:require [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc]
            [clojure.pprint :refer [pprint]]
            [taoensso.timbre :as timbre]
            [clojure.spec.alpha :as s]
            [com.stuartsierra.component :as component]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.set :as set]
            [om.next :as om]
            [om.util :as util])
  (:import (org.flywaydb.core Flyway)
           (com.zaxxer.hikari HikariConfig HikariDataSource)
           (java.util Properties)
           (org.slf4j LoggerFactory)))

(s/def ::driver keyword?)
(s/def ::joins (s/and map?
                 #(every? keyword? (keys %))
                 #(every? vector? (vals %))))
(s/def ::pks (s/and map?
               #(every? keyword? (keys %))
               #(every? keyword? (vals %))))
(s/def ::graph->sql (s/and map?
                      #(every? keyword? (keys %))
                      #(every? keyword? (vals %))))

(s/def ::schema (s/keys
                  :req [::pks ::graph->sql ::joins]
                  :opt [::driver]))

(defmulti graphprop->sqlprop* (fn sqlize-dispatch [schema kw] (get schema :driver :default)))

(defmethod graphprop->sqlprop* :default [schema kw]
  (let [nspc (some-> kw namespace (str/replace "-" "_"))
        nm   (some-> kw name (str/replace "-" "_"))]
    (if nspc
      (keyword nspc nm)
      (keyword nm))))

(defn graphprop->sqlprop
  "Convert a keyword in clojure-form to sql-form. E.g. :account-id to :account_id"
  [schema kw]
  (graphprop->sqlprop* schema kw))

(defmulti sqlprop->graphprop* (fn omize-dispatch [schema kw] (get schema :driver :default)))

(defmethod sqlprop->graphprop* :default [schema kw]
  (let [nspc (some-> kw namespace (str/replace "_" "-"))
        nm   (some-> kw name (str/replace "_" "-"))]
    (if nspc
      (keyword nspc nm)
      (keyword nm))))

(defn sqlprop->graphprop
  "Convert a keyword in clojure-form to sql-form. E.g. :account-id to :account_id"
  [schema kw]
  (sqlprop->graphprop* schema kw))

(defn omprop->sqlprop
  "Derive an sqlprop from an om query element (prop or join)"
  [{:keys [::graph->sql] :as schema} p]
  (graphprop->sqlprop schema
    (if (map? p)
      (get graph->sql (ffirst p) (ffirst p))
      (get graph->sql p p))))

(defmulti table-for* (fn [schema query] (get schema :driver :default)))

(defmethod table-for* :default
  [schema query]
  (let [nses (reduce (fn
                       ([] #{})
                       ([s p]
                        (let [sql-prop (omprop->sqlprop schema p)
                              table-kw (some-> sql-prop namespace keyword)]
                          (cond
                            (= :id sql-prop) s
                            (= :db/id sql-prop) s
                            table-kw (conj s table-kw)
                            :else s)))) #{} query)]
    (assert (= 1 (count nses)) (str "Could not determine a single table from the subquery " query))
    (graphprop->sqlprop schema (first nses))))

(defn table-for
  "Scans the given Om query and tries to determine which table is to be used for the props within it."
  [schema query]
  (assert (s/valid? ::schema schema) "Schema is valid")
  (table-for* schema query))

(defn id-columns
  "Returns a set of table-namespaced keywords that are the ID columns for all tables."
  [{:keys [::pks] :as schema}]
  (reduce (fn [cset [table pk]]
            (conj cset (keyword (name table) (name pk))))
    #{} pks))

(defmulti column-spec*
  "Get the database-specific column query specification for a given SQL prop."
  (fn [schema sqlprop] (get schema ::driver :default)))

(defmethod column-spec* :default
  [schema sqlprop]
  (let [table   (namespace sqlprop)
        col     (name sqlprop)
        as-name (str (namespace sqlprop) "/" (name sqlprop))]
    (str table "." col " AS \"" as-name "\"")))

(defn column-spec
  "Returns a database-specific SQL property selection and AS clause for the given sql prop.

  E.g.: (column-spec schema :account/name) => account.name AS \"account/name\"
  "
  [schema sqlprop]
  (assert (s/valid? ::schema schema) "schema is valid")
  (column-spec* schema sqlprop))

(s/def ::migrations (s/and vector? #(every? string? %)))
(s/def ::hikaricp-config string?)

(s/def ::db (s/keys
              :req-un [::hikaricp-config ::migrations]
              :opt-un [::create-drop? ::auto-migrate?]))
(s/def ::sqldbm (s/and map?
                  #(every? keyword? (keys %))
                  #(every? (fn [db] (s/valid? ::db db)) (vals %))))

(defprotocol SQLDatabaseManager
  (start-databases [this] "Create the connection pools and (optionally) run the migrations.")
  (get-dbspec [this database-kw] "Get a clojure jdbc dbspec for the given database-kw."))

(defn create-pool
  "Create a HikariDataSource for connection pooling from a properties filename."
  [^String properties-file]
  (try
    (let [source            (if (str/starts-with? properties-file "/")
                              properties-file
                              (io/resource properties-file))
          reader            (io/reader source)
          ^Properties props (Properties.)]
      (with-open [reader reader]
        (.load props reader))
      (let [^HikariConfig config (HikariConfig. props)]
        (HikariDataSource. config)))
    (catch Exception e
      (timbre/error "Unable to create Hikari Datasource: " (.getMessage e)))))

(defmulti create-drop* (fn [db dbkey dbconfig] (get dbconfig :driver :default)))

(defmethod create-drop* :default [db dbkey dbconfig]
  (timbre/info "Create-drop was set. Cleaning everything out of the database " dbkey " (PostgreSQL).")
  (jdbc/execute! db ["DROP SCHEMA PUBLIC CASCADE"])
  (jdbc/execute! db ["CREATE SCHEMA PUBLIC"]))

(defmethod create-drop* :h2 [db dbkey dbconfig]
  (timbre/info "Create-drop was set. Cleaning everything out of the database " dbkey " (PostgreSQL).")
  (jdbc/execute! db ["DROP ALL OBJECTS"]))

(defmethod create-drop* :mysql [db dbkey dbconfig]
  (let [name (get dbconfig :database-name (name dbkey))]
    (timbre/info "Create-drop was set. Cleaning everything out of the database " name " (MySQL).")
    (jdbc/execute! db [(str "DROP DATABASE " name)])
    (jdbc/execute! db [(str "CREATE DATABASE " name)])
    (jdbc/execute! db [(str "USE " name)])
    (timbre/info "Create-drop complete.")))

(defrecord DatabaseManager [config connection-pools]
  component/Lifecycle
  (start [this]
    (let [databases (-> config :value :sqldbm)
          valid?    (s/valid? ::sqldbm databases)
          pools     (and valid?
                      (reduce (fn [pools [dbkey dbconfig]]
                                (timbre/info (str "Creating connection pool for " dbkey))
                                (assoc pools dbkey (create-pool (:hikaricp-config dbconfig)))) {} databases))
          result    (assoc this :connection-pools pools)]
      (try
        (if valid?
          (start-databases result)
          (timbre/error "Unable to start SQL Databases. Configuration is invalid: " (s/explain ::sqldbm databases)))
        (catch Throwable t
          (timbre/error "DATABASE STARTUP FAILED: " t)
          (component/stop result)))
      result))
  (stop [this]
    (doseq [[k ^HikariDataSource p] connection-pools]
      (timbre/info "Shutting down pool " k)
      (.close p))
    (assoc this :connection-pools []))
  SQLDatabaseManager
  (start-databases [this]
    (let [database-map (some-> config :value :sqldbm)]
      (doseq [[dbkey dbconfig] database-map]
        (let [{:keys [create-drop? auto-migrate? migrations]} dbconfig
              ^HikariDataSource pool (get connection-pools dbkey)
              db                     {:datasource pool}]
          (if pool
            (do
              (timbre/info (str "Processing migrations for " dbkey))

              (when-let [^Flyway flyway (when auto-migrate? (Flyway.))]
                (when create-drop? (create-drop* db dbkey dbconfig))
                (timbre/info "Migration location is set to: " migrations)
                (.setLocations flyway (into-array String migrations))
                (.setDataSource flyway pool)
                (.migrate flyway)))
            (timbre/error (str "No pool for " dbkey ". Skipping migrations.")))))))
  (get-dbspec [this kw] (some->> connection-pools kw (assoc {} :datasource))))

(defn build-db-manager
  "Build a component that can manage you SQL database startup and stop."
  [config] (map->DatabaseManager {:config config}))

(defmulti next-id*
  (fn next-id-dispatch [db schema table] (get schema :driver :default)))

(defmethod next-id* :mysql
  [db schema table]
  (assert (s/valid? ::schema schema) "Next-id requires a valid schema.")
  (jdbc/with-db-transaction [db db]
    (let [next-id (jdbc/query db ["SELECT AUTO_INCREMENT AS \"id\" FROM information_schema.TABLES WHERE TABLE_SCHEMA = database() AND TABLE_NAME = ?" (name table)]
                    {:result-set-fn first :row-fn :id})]
      (jdbc/execute! db [(str "ALTER TABLE " (name table) " AUTO_INCREMENT = " (inc next-id))])
      next-id)))

(defmethod next-id* :postgresql
  [db schema table]
  (assert (s/valid? ::schema schema) "Next-id requires a valid schema.")
  (let [pk      (get-in schema [::pks table] :id)
        seqname (str (name table) "_" (name pk) "_seq")]
    (jdbc/query db [(str "SELECT nextval('" seqname "') AS \"id\"")]
      {:result-set-fn first
       :row-fn        :id})))

(defmethod next-id* :default
  [db schema table]
  (next-id* db (assoc schema :driver :postgresql) table))

(defn next-id
  "Get the next generated ID for the given table.

  NOTE: IF you specify the Java System Property `dev`, then this function will assume you are writing tests and will
  allocate extra IDs in order to prevent assertions on your generated IDs across
  tables from giving false positives (since all tables will start from ID 1). It does this by throwing away a
  random number of IDs, so that IDs across tables are less likely to be identical when an equal number of rows
  are inserted."
  [db schema table-kw]
  (let [n (rand-int 20)]
    (when (System/getProperty "dev")
      (doseq [r (range n)]
        (next-id* db schema table-kw)))
    (next-id* db schema table-kw)))

(defn seed-row
  "Generate an instruction to insert a seed row for a table, which can contain keyword placeholders for IDs. It is
   recommended you namespace your generated IDs into `id` so that substitution during seeding doesn't cause surprises.
   For example:

  ```
  (seed-row :account {:id :id/joe ...})
  ```

  If the generated IDs appear in a PK location, they will be generated (must be unique per seed set). If they
  are in a value column, then the current generated value (which must have already been seeded) will be used.

  See also `seed-update` for resolving circular references.
  "
  [table value]
  (with-meta value {:table table}))

(defn seed-update
  "Generates an instruction to update a seed row (in the same seed set) that already appeared. This may be necessary if your database has
  referential loops.

  ```
  (seed-row :account {:id :id/joe ...})
  (seed-row :account {:id :id/sam ...})
  (seed-update :account :id/joe {:last_edited_by :id/sam })
  ```

  `table` should be a keyword form of the table in your database.
  `id` can be a real ID or a generated ID placeholder keyword (recommended: namespace it with `id`).
  `value` is a map of col/value pairs to update on the row.
  "
  [table id value]
  (with-meta value {:update id :table table}))

(defn join-key
  "Returns the key in a join. E.g. for {:k [...]} it returns :k"
  [join] (ffirst join))
(defn join-query
  "Returns the subquery of a join. E.g. for {:k [:data]} it returns [:data]."
  [join] (-> join first second))

(defn pk-column
  "Returns the SQL column for a given table's primary key"
  [schema table]
  (get-in schema [::pks table] :id))

(defn id-prop
  "Returns the SQL-centric property for the PK in a result set map (before conversion back to Om)"
  [schema table]
  (keyword (name table) (name (pk-column schema table))))

(defn seed!
  "Seed the given seed-row and seed-update items into the given database. Returns a map whose values will be the
  keyword placeholders for generated PK ids, and whose values are the real numeric generated ID:

  ```
  (let [{:keys [id/sam id/joe]} (seed! db schema [(seed-row :account {:id :id/joe ...})
                                                  (seed-row :account {:id :id/sam ...})]
    ...)
  ```
  "
  [db schema rows]
  (assert (s/valid? ::schema schema) "Schema is not valid")
  (let [tempid-map (reduce (fn [kws r]
                             (let [{:keys [update table]} (meta r)
                                   id (get r (pk-column schema table))]
                               (assert (or update id) "Expected an update or the row to contain the primary key")
                               (cond
                                 update kws
                                 (keyword? id) (assoc kws id (next-id db schema table))
                                 :else kws
                                 ))) {} rows)
        remap-row  (fn [row] (clojure.walk/postwalk (fn [e] (if (keyword? e) (get tempid-map e e) e)) row))]
    (doseq [row rows]
      (let [{:keys [update table]} (meta row)
            real-row (remap-row row)
            pk       (pk-column schema table)
            pk-val   (if update
                       (get tempid-map update update)
                       (get row pk))]
        (if update
          (do
            (timbre/debug "updating " row "at" pk pk-val)
            (jdbc/update! db table real-row [(str (name pk) " = ?") pk-val]))
          (do
            (timbre/debug "inserting " real-row)
            (jdbc/insert! db table real-row)))))
    tempid-map))

(defn query-element->sqlprop
  [{:keys [::joins] :as schema} element]
  (let [omprop                 (if (map? element) (ffirst element) element)
        id-columns             (id-columns schema)
        sql-prop               (omprop->sqlprop schema omprop)
        join                   (get joins omprop)
        join-source-is-row-id? (some->> join first (contains? id-columns) boolean)
        join-col               (first join)
        join-prop              (when join-col (omprop->sqlprop schema join-col))]
    (cond
      (= "id" (name sql-prop)) nil
      join-prop join-prop
      :else sql-prop)))

(defn columns-for
  "Returns an SQL-centric set of properties at the top level of the given graph query. It does not follow joins, but
  does include any columns that would be necessary to process the given joins. It will always include the row ID."
  [schema graph-query]
  (assert (s/valid? ::schema schema) "Schema is valid")
  (let [table  (table-for schema graph-query)
        pk     (get-in schema [::pks table] :id)
        id-col (keyword (name table) (name pk))]
    (reduce
      (fn [rv ele]
        (if-let [prop (query-element->sqlprop schema ele)]
          (conj rv prop)
          rv)) #{id-col} graph-query)))

(defn str-idcol
  "Returns the SQL string for the ID column of the given (keyword) table. E.g. :account -> account.id"
  [schema table]
  (str (name table) "." (name (pk-column schema table))))

(defn str-col
  "Returns the SQL string for the given sqlprop. E.g. :a/b -> a.b"
  [prop]
  (str (namespace prop) "." (name prop)))

(defn sqlprop-for-join
  "Returns the sqlprop column needed from the source table."
  [{:keys [::joins] :as schema} join]
  (let [jk               (omprop->sqlprop schema (ffirst join))
        join-description (get joins jk)]
    (first join-description)))

(defn filtering-expression [schema column instruction]
  (assert (map? instruction) "Filtering instruction is a map of the form {:op val}")
  (let [table-name (name (table-for schema [column]))
        sql-name   (name (query-element->sqlprop schema column))
        [op val] (first instruction)
        op         (case op
                     :eq "="
                     :gt ">"
                     :lt "<"
                     :ne "<>")]
    [(str table-name "." sql-name " " op " ?") val]))

(defn row-filter
  "Generate a row filter based on the filtering configuration and a set of table names (strings)"
  [schema filtering table-set]
  (let [is-on-table? (fn is-on-table? [p]
                       (contains? table-set (some->> p vector (table-for schema) name)))
        filter-cols  (filter is-on-table? (keys filtering))
        [clauses params] (reduce
                           (fn filter-step [[clause params] col]
                             (let [instruction (get filtering col)
                                   [subclause param] (filtering-expression schema col instruction)]
                               [(conj clause subclause) (conj params param)]))
                           [[] []] filter-cols)]
    (if (seq clauses)
      [(str/join " AND " clauses) params]
      [nil nil])))

(defn query-for
  "Returns an SQL query to get the true data columns that exist for the graph-query. Joins will contribute to this
  query iff there is a column on the target table that is needed in order to process the join.

  graph-join-prop : nil if the id-set is on the table of this query itself, otherwise the fulcro query keyword that was used to follow a join.
  graph-query : the things to pull from the databsae. Table will be derived from this, so you must pull more than just the ID.
  id-set : The ids of the rows you want to pull. If join-col is set, that will be the join column matched against these. Otherwise the PK of the table."
  ([schema graph-join-prop graph-query id-set] (query-for schema graph-join-prop graph-query id-set {}))
  ([{:keys [::joins] :as schema} graph-join-prop graph-query id-set filtering]
   (assert (vector? graph-query) "Om query is a vector: ")
   (assert (or (nil? graph-join-prop) (keyword? graph-join-prop)) "join column is a keyword")
   (assert (set? id-set) "id-set is a set")
   (let [table                (table-for schema graph-query)
         join-cols-to-include (->> graph-query
                                (filter map?)
                                (keep (partial sqlprop-for-join schema)))
         join-path            (get joins graph-join-prop)
         sql-join-column      (second join-path)
         columns              (apply sorted-set (concat (columns-for schema graph-query) join-cols-to-include))
         columns              (if sql-join-column (conj columns sql-join-column) columns)
         column-selectors     (map #(column-spec schema %) columns)
         selectors            (str/join "," column-selectors)
         table-name           (name table)
         ids                  (str/join "," (map str (keep identity id-set)))
         has-join-table?      (> (count join-path) 2)
         id-col               (if sql-join-column
                                (str-col sql-join-column)
                                (str-idcol schema table))
         [general-filter params] (row-filter schema filtering #{table-name})]
     (if has-join-table?
       (let [left-table  (-> join-path second namespace)
             right-table (-> join-path last namespace)
             col-left    (-> join-path (nth 2) str-col)
             col-right   (-> join-path (nth 3) str-col)
             filter-col  (-> join-path second str-col)
             [general-filter params] (row-filter schema filtering #{left-table right-table})
             from-clause (str "FROM " left-table " INNER JOIN " right-table " ON " col-left " = " col-right)
             ids         (str/join "," id-set)]
         [(str "SELECT " selectors " " from-clause " WHERE " (when general-filter (str general-filter " AND ")) filter-col " IN (" ids ")") params])
       [(str "SELECT " selectors " FROM " table-name " WHERE " (when general-filter (str general-filter " AND ")) id-col " IN (" ids ")") params]))))

(defn to-one [join-seq]
  (assert (and (vector? join-seq) (every? keyword? join-seq)) "join sequence is a vector of keywords")
  (vary-meta join-seq assoc :arity :to-one))

(defn to-many [join-seq]
  (assert (and (vector? join-seq) (every? keyword? join-seq)) "join sequence is a vector of keywords")
  (vary-meta join-seq assoc :arity :to-many))

(defn to-one?
  "Is the give join to-one? Returns true iff the join is marked to-one."
  [join]
  (= :to-one (some-> join meta :arity)))

(defn to-many?
  "Is the given join to-many? Returns true if the join is marked to many, or if the join is unmarked (e.g. default)"
  [join]
  (or (= :to-many (some-> join meta :arity)) (not (to-one? join))))

(defn recursive?
  "Is this a self-reference join?"
  [{:keys [::joins] :as schema} graph-key-or-join]
  (let [graph-join-prop (if (map? graph-key-or-join) (join-key graph-key-or-join)
                                                     graph-key-or-join)
        join-sequence   (get joins graph-join-prop)
        source-table    (keyword (namespace (first join-sequence)))
        target-table    (keyword (namespace (second join-sequence)))]
    (= source-table target-table)))

(defn reverse?
  "Opposite of forward?"
  [{:keys [::joins] :as schema} graph-key-or-join]
  (let [graph-join-prop (if (map? graph-key-or-join) (join-key graph-key-or-join)
                                                     graph-key-or-join)
        join-sequence   (get joins graph-join-prop)
        source-table    (keyword (namespace graph-join-prop))
        source-pk-col   (pk-column schema source-table)
        source-pk       (keyword (name source-table) (name source-pk-col))
        join-start      (first join-sequence)]
    (= join-start source-pk)))

(defn forward?
  "Returns true if the join key is on the source table (as opposed to the target table)"
  [schema graph-join]
  (not (reverse? schema graph-join)))

(defn uses-join-table?
  "Returns true if the join has a join table"
  [{:keys [::joins] :as schema} graph-key-or-join]
  (let [graph-join-prop (if (map? graph-key-or-join) (join-key graph-key-or-join)
                                                     graph-key-or-join)
        join-sequence   (get joins graph-join-prop)
        ]
    (= 4 (count join-sequence))))

(defn- run-query*
  ([db schema join-or-id-column query filtering root-id-set] (run-query* db schema join-or-id-column query filtering root-id-set {}))
  ([db {:keys [::joins] :as schema} join-or-id-column query filtering root-id-set recursion-tracking]
    ; NOTE: recursion-tracking is a map: keys are the join key followed through recursion, value is the set of ids.
    ; a loop is detected when the intersection of the new id set and the old id set is not empty
   (assert (s/valid? ::schema schema) "schema is valid")
   (when (seq root-id-set)
     (let [is-join?                 (contains? joins join-or-id-column)
           join-path                (get joins join-or-id-column [])
           id-column                (if is-join? (second join-path) join-or-id-column)
           query-joins              (keep #(when (map? %) %) query)
           [sql params] (query-for schema (when is-join? join-or-id-column) query root-id-set filtering)
           rows                     (jdbc/query db (into [sql] params))
           get-root-set             (fn [join]
                                      (let [join-key      (ffirst join)
                                            join-sequence (get joins join-key [])
                                            root-set-prop (first join-sequence)]
                                        (if root-set-prop
                                          (reduce (fn [s row]
                                                    (if-let [id (get row root-set-prop)]
                                                      (conj s id)
                                                      s)) #{} rows)
                                          #{})))
           join-results             (reduce
                                      (fn [acc query-join]
                                        (let [subquery            (join-query query-join)
                                              recursive?          (or (= subquery '...) (integer? subquery))
                                              recursive-depth     (if (integer? subquery) subquery 1)
                                              k                   (join-key query-join)
                                              real-query          (if recursive?
                                                                    (om/reduce-query-depth query k)
                                                                    subquery)
                                              root-set            (get-root-set query-join)
                                              loop?               (not-empty (set/intersection (get recursion-tracking k) root-set))
                                              updated-recur-track (if (and (not loop?) recursive?)
                                                                    (update recursion-tracking k (fnil #(set/union % root-set) #{}))
                                                                    recursion-tracking)
                                              results             (if (or loop? (< recursive-depth 1))
                                                                    nil ; this should be a {:table/id id} marker
                                                                    (run-query* db schema k real-query filtering root-set updated-recur-track))
                                              join-sequence       (get joins k)
                                              fkid-col            (second join-sequence)
                                              grouped-results     (group-by fkid-col results)
                                              grouped-results     (if (map? results)
                                                                    {(get results fkid-col) results}
                                                                    grouped-results)]
                                          (assoc acc (join-key query-join) grouped-results)))
                                      {}
                                      query-joins)
           join-row-to-join-results (fn [row]
                                      (reduce
                                        (fn [r [jk grouped-results]]
                                          (let [join-path   (get joins jk)
                                                forward-key (first join-path)
                                                row-key     (cond
                                                              (uses-join-table? schema jk) forward-key
                                                              (forward? schema jk) forward-key
                                                              (recursive? schema jk) forward-key
                                                              :else id-column)
                                                row-id      (get r row-key)
                                                join-result (get grouped-results row-id)
                                                join-result (if (to-one? join-path)
                                                              (first join-result)
                                                              join-result)]
                                            (if join-result
                                              (assoc r jk join-result)
                                              r)))
                                        row join-results))
           final-results            (map join-row-to-join-results rows)]
       (vec final-results)))))

(defn strip-join-columns
  "Walk the query and graph result, removing any join columns that were part of query processing, but were not asked
  for in the original query."
  [query graph-result]
  (if (vector? graph-result)
    (mapv #(strip-join-columns query %) graph-result)
    (let [legal-keys   (keep (fn [ele] (if (map? ele) (ffirst ele) ele)) query)
          this-result  (select-keys graph-result legal-keys)
          final-result (reduce (fn [r query-ele]
                                 (let [is-join?   (map? query-ele)
                                       key        (if is-join? (join-key query-ele) query-ele)
                                       subquery   (and is-join? (join-query query-ele))
                                       subquery   (if (util/recursion? subquery) query subquery)
                                       join-value (get r key)
                                       to-many?   (and is-join? (vector? join-value))]
                                   (cond
                                     (and to-many? join-value) (assoc r key (mapv (fn [v] (strip-join-columns subquery v)) join-value))
                                     (and is-join? join-value) (assoc r key (strip-join-columns subquery join-value))
                                     :else r))) this-result query)]
      final-result)))

(defn run-query
  "Run a graph query against an SQL database.

  db - the database
  schema - the schema
  join-or-id-column - The ID column of the table (being queried) corresponding to root-id-set OR the join column
                      whose second join component corresponds to the IDs in root-id-set.
  query - The query to run
  root-id-set - A set of PK values that identify the row(s) that root your graph query

  Returns:
  - IF the join-or-id-column is a to-one join: returns a map
  - Otherwise returns a vector of maps, one entry for each ID in root-id-set
  "
  ([db {:keys [::joins ::graph->sql] :as schema} join-or-id-column query root-id-set] (run-query db schema join-or-id-column query root-id-set {}))
  ([db {:keys [::joins ::graph->sql] :as schema} join-or-id-column query root-id-set filtering]
   (try
     (jdbc/with-db-transaction [db db]
       (let [sql-results   (run-query* db schema join-or-id-column query filtering root-id-set)
             sql->graph    (set/map-invert graph->sql)
             graph-results (clojure.walk/postwalk (fn [ele]
                                                    (cond
                                                      (and (keyword? ele) (= "id" (name ele))) :db/id
                                                      (keyword? ele) (sqlprop->graphprop schema (get sql->graph ele ele))
                                                      :else ele)) sql-results)]
         (strip-join-columns query graph-results)))
     (catch Throwable t
       (timbre/error "Graph query failed: " t)
       (.printStackTrace t)))))


