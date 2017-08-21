(defproject fulcro-sql "0.1.0-SNAPSHOT"
  :description "A library for running Om graph queries against an SQL database."
  :url "http://github.com/fulcrologic/fulcro-sql"
  :license {:name "MIT Public License"}

  :source-paths ["src/main"]
  :test-paths ["src/test"]

  :profiles {:dev {:source-paths ["src/main" "src/dev"]}}

  :dependencies [[org.clojure/clojure "1.8.0" :scope "provided"]
                 [org.omcljs/om "1.0.0-beta1" :scope "provided"]
                 [fulcrologic/fulcro-spec "1.0.0-beta8" :scope "test"]])
