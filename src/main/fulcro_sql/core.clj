(ns fulcro-sql.core
  (:require [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc]
            [taoensso.timbre :as timbre]
            [clojure.spec.alpha :as s]
            [com.stuartsierra.component :as component]
            [clojure.java.io :as io]
            [clojure.string :as str])
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
(s/def ::om->sql (s/and map?
                   #(every? keyword? (keys %))
                   #(every? keyword? (vals %))))

(s/def ::schema (s/keys
                  :req [::pks ::om->sql ::joins]
                  :opt [::driver]))

(defmulti sqlize* (fn sqlize-dispatch [schema kw] (get schema :driver :default)))

(defmethod sqlize* :default [schema kw]
  (let [nspc (some-> kw namespace (str/replace "-" "_"))
        nm   (some-> kw name (str/replace "-" "_"))]
    (if nspc
      (keyword nspc nm)
      (keyword nm))))

(defn sqlize
  "Convert a keyword in clojure-form to sql-form. E.g. :account-id to :account_id"
  [schema kw]
  (sqlize* schema kw))

(defmulti table-for* (fn [schema query] (get schema :driver :default)))

(defmethod table-for* :default
  [schema query]
  (let [{:keys [om->sql]} schema
        nses (reduce (fn
                       ([] #{})
                       ([s p]
                        (let [sql-prop (if (map? p) nil (get om->sql p p))
                              table-kw (some-> sql-prop namespace keyword)]
                          (cond
                            (map? p) s
                            (= :db/id sql-prop) s
                            (and sql-prop table-kw) (conj s table-kw)
                            :else s)))) #{} query)]
    (assert (= 1 (count nses)) (str "Could not determine a single table from the subquery " query))
    (sqlize schema (first nses))))

(defn table-for
  "Scans the given Om query and tries to determine which table is to be used for the props within it."
  [schema query]
  (table-for* schema query))

(defn id-columns
  "Returns a set of table-namespaced keywords that are the ID columns for all tables."
  [{:keys [::pks] :as schema}]
  (reduce (fn [cset [table pk]]
            (conj cset (keyword (name table) (name pk))))
    #{} pks))

(defmulti column-spec
  "Get the column query specification for a given om prop. The omprop will be converted to an SQL :table/col property
  that may be a scalar value on `table` or a join. If it is a join, it will only return a value if the join has
  data on the `table` implied (e.g. isn't a reverse FK reference). "
  (fn [schema omprop] (get schema ::driver :default)))

(defmethod column-spec :default
  [{:keys [::om->sql ::joins] :as schema} omprop]
  (let [id-columns             (id-columns schema)
        sqlprop                (sqlize schema (get om->sql omprop omprop))
        join                   (get joins sqlprop)
        join-source-is-row-id? (some->> join first (contains? id-columns) boolean)
        join-col               (first join)
        [sqltable column target] (if (seq join)
                                   [(namespace join-col) (name join-col) sqlprop]
                                   [(namespace sqlprop) (name sqlprop) omprop])
        as-name                (str (namespace target) "/" (name target))]
    (str sqltable "." column " AS \"" as-name "\"")))

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

(defrecord PostgreSQLDatabaseManager [config connection-pools]
  component/Lifecycle
  (start [this]
    (timbre/debug "Ensuring PostgreSQL JDBC driver is loaded.")
    (Class/forName "org.postgresql.Driver")
    (let [databases (-> config :value :sqldbm)
          ok?       (s/valid? ::sqldbm databases)
          pools     (and ok?
                      (reduce (fn [pools [dbkey dbconfig]]
                                (timbre/info (str "Creating connection pool for " dbkey))
                                (assoc pools dbkey (create-pool (:hikaricp-config dbconfig)))) {} databases))
          result    (assoc this :connection-pools pools)]
      (if ok?
        (start-databases result)
        (timbre/error "Unable to start SQL Databases. Configuration is invalid: " (s/explain ::sqldbm databases)))
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
                (when create-drop?
                  (timbre/info "Create-drop was set. Cleaning everything out of the database.")
                  (jdbc/execute! db ["DROP SCHEMA PUBLIC CASCADE"])
                  (jdbc/execute! db ["CREATE SCHEMA PUBLIC"]))
                (timbre/info "Migration location is set to: " migrations)
                (.setLocations flyway (into-array String migrations))
                (.setDataSource flyway pool)
                (.migrate flyway)))
            (timbre/error (str "No pool for " dbkey ". Skipping migrations.")))))))
  (get-dbspec [this kw] (some->> connection-pools kw (assoc {} :datasource))))

(defmulti next-id*
  (fn next-id-dispatch [db schema table] (get schema :driver :default)))

(defmethod next-id* :default
  [db schema table]
  (assert (s/valid? ::schema schema) "Next-id requires a valid schema.")
  (let [pk      (get-in schema [::pks table] :id)
        seqname (str (name table) "_" (name pk) "_seq")]
    (jdbc/query db [(str "SELECT nextval('" seqname "') AS \"id\"")]
      {:result-set-fn first
       :row-fn        :id})))

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

(defn pk-column [schema table]
  (get-in schema [::pks table] :id))

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
