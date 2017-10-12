(defproject fulcrologic/fulcro-sql "0.2.0-SNAPSHOT"
  :description "A library for using SQL databases as components, writing integration tests, and running Datomic-style graph queries against them."
  :url "http://github.com/fulcrologic/fulcro-sql"
  :license {:name "MIT Public License"}

  :source-paths ["src/main"]
  :test-paths ["src/test"]

  :profiles {:dev {:source-paths   ["src/main" "src/dev"]
                   :resource-paths ["resources" "test-resources"]}}

  :dependencies [[org.clojure/clojure "1.8.0" :scope "provided"]
                 [org.omcljs/om "1.0.0-beta1" :scope "provided"]
                 [clojure-future-spec "1.9.0-alpha17"]
                 [org.flywaydb/flyway-core "4.2.0"]
                 [com.zaxxer/HikariCP "2.6.3"]
                 [com.stuartsierra/component "0.3.2"]
                 [org.clojure/java.jdbc "0.7.0"]
                 [com.taoensso/timbre "4.10.0"]

                 ; Logging: If you include these dependencies you can forward
                 ; SLF4J logging through timbre (Flyway and Hikari both use and autodetect slf4j)
                 [org.slf4j/log4j-over-slf4j "1.7.25" :scope "provided"]
                 [org.slf4j/jul-to-slf4j "1.7.25" :scope "provided"]
                 [org.slf4j/jcl-over-slf4j "1.7.25" :scope "provided"]
                 [com.fzakaria/slf4j-timbre "0.3.7" :scope "provided"]

                 [fulcrologic/fulcro-spec "1.0.0-beta8" :scope "test" :exclusions [org.clojure/tools.reader]]
                 [clj-time "0.14.0" :scope "test"]
                 [org.mariadb.jdbc/mariadb-java-client "2.1.0" :scope "test"]
                 [org.postgresql/postgresql "42.1.4.jre7" :scope "test"]
                 [com.h2database/h2 "1.4.196" :scope "test"]])
