(ns fulcro-sql.test-helpers
  (:require
    [clojure.java.jdbc :as jdbc]
    [fulcro-sql.core :as core]
    [clojure.spec.alpha :as s]
    [com.stuartsierra.component :as component]))

(defmacro with-database
  "Start databases for the given name-to-config bindings, started and migrate the databases, run the given body,
   then shut down the database.

   The `bindings` should be pairs of `sym` and `config`

   `sym` - Any symbol you want to bind to
   `config` - A map containing the database configuration. NOTE: :auto-migrate? will be forced to true, as will :create-drop?"
  [binding & body]
  (let [sym  (first binding)
        dbkw (keyword (name sym))]
    `(let [config# {:value
                    {:sqldbm
                     {~dbkw (assoc ~(second binding)
                              :create-drop? true
                              :auto-migrate? true)}}}
           dbs#    (atom (core/build-db-manager config#))]
       (try
         (swap! dbs# component/start)
         (let [~sym (core/get-dbspec (deref dbs#) ~dbkw)]
           ~@body)
         (finally
           (swap! dbs# component/stop))))))

(comment
  (macroexpand-1 '(with-database [a {:x a}]
                    (jdbc/insert! a :blah {:boo "value"}))))
