(ns fulcro-sql.core-spec
  (:require
    [fulcro-spec.core :refer [assertions specification behavior when-mocking component]]
    [fulcro-sql.core :as core]
    [fulcro-sql.test-helpers :refer [with-database]]
    [clojure.spec.alpha :as s]
    [clj-time.core :as tm]
    [com.stuartsierra.component :as component]
    [clojure.java.jdbc :as jdbc]
    [clj-time.jdbc]
    [taoensso.timbre :as timbre])
  (:import (com.cognitect.transit TaggedValue)))

(def test-database {:hikaricp-config "test.properties"
                    :migrations      ["classpath:migrations/test"]})

(def mysql-database {:hikaricp-config "mysqltest.properties"
                     :driver          :mysql
                     :database-name   "test"
                     :migrations      ["classpath:migrations/mysqltest"]})

(def h2-database {:hikaricp-config "h2test.properties"
                  :driver          :h2
                  :database-name   "test"
                  :migrations      ["classpath:migrations/h2test"]})

(def test-schema {::core/graph->sql {:person/name                  :member/name
                                     :person/account               :member/account_id
                                     :settings/auto-open?          :settings/auto_open
                                     :settings/keyboard-shortcuts? :settings/keyboard_shortcuts}
                  ; NOTE: Om join prop, SQL column props
                  ::core/joins      {:account/members         (core/to-many [:account/id :member/account_id])
                                     :account/settings        (core/to-one [:account/settings_id :settings/id])
                                     :account/spouse          (core/to-one [:account/spouse_id :account/id])
                                     :member/account          (core/to-one [:member/account_id :account/id])
                                     :account/invoices        (core/to-many [:account/id :invoice/account_id])
                                     :invoice/account         (core/to-one [:invoice/account_id :account/id])
                                     :invoice/items           (core/to-many [:invoice/id :invoice_items/invoice_id :invoice_items/item_id :item/id])
                                     :item/invoices           (core/to-many [:item/id :invoice_items/item_id :invoice_items/invoice_id :invoice/id])

                                     :todo-list/items         (core/to-many [:todo_list/id :todo_list_item/todo_list_id])
                                     :todo-list-item/subitems (core/to-many [:todo_list_item/id :todo_list_item/parent_item_id])}
                  ; sql table -> id col
                  ::core/pks        {}})
(def mysql-schema
  (assoc test-schema
    :driver :mysql
    :database-name "test"))                                 ; needed for create/drop in mysql...there is no drop schema

(def h2-schema
  (assoc test-schema :driver :h2))

(specification "Database Component" :integration
  (behavior "Can create a functional database pool from HikariCP properties and Flyway migrations."
    (with-database [db test-database]
      (let [result (jdbc/with-db-connection [con db] (jdbc/insert! con :account {:name "Tony"}))
            row    (first result)]
        (assertions
          (:name row) => "Tony")))))

(specification "next-id (MySQL)" :mysql
  (behavior "Pulls a monotonically increasing ID from the database (MySQL/MariaDB)"
    (with-database [db mysql-database]
      (let [a (core/next-id db mysql-schema :account)
            b (core/next-id db mysql-schema :account)]
        (assertions
          "IDs are numeric integers > 0"
          (> a 0) => true
          (> b 0) => true
          "IDs are increasing"
          (> b a) => true)))))

(specification "next-id (PostgreSQL)" :integration
  (behavior "Pulls a monotonically increasing ID from the database (PostgreSQL)"
    (with-database [db test-database]
      (let [a (core/next-id db test-schema :account)
            b (core/next-id db test-schema :account)]
        (assertions
          "IDs are numeric integers > 0"
          (> a 0) => true
          (> b 0) => true
          "IDs are increasing"
          (> b a) => true)))))

(specification "seed!" :integration
  (with-database [db test-database]
    (jdbc/with-db-connection [db db]
      (let [rows     [(core/seed-row :account {:id :id/joe :name "Joe"})
                      (core/seed-row :member {:id :id/sam :account_id :id/joe :name "Sam"})
                      (core/seed-update :account :id/joe {:last_edited_by :id/sam})]
            {:keys [id/joe id/sam] :as tempids} (core/seed! db test-schema rows)
            real-joe (jdbc/get-by-id db :account joe)
            real-sam (jdbc/get-by-id db :member sam)]
        (assertions
          "Temporary IDs are returned for each row"
          (pos? joe) => true
          (pos? sam) => true
          "The data is inserted into the database"
          (:name real-joe) => "Joe"
          (:last_edited_by real-joe) => sam)))))

(specification "Table Detection: `table-for`"
  (let [schema {::core/pks        {}
                ::core/joins      {}
                ::core/graph->sql {:thing/name    :sql_table/name
                                   :boo/blah      :sql_table/prop
                                   :the-thing/boo :the-table/bah}}]
    (assertions
      ":id and :db/id are ignored"
      (core/table-for schema [:account/name :db/id :id]) => :account
      "Detects the correct table name if all of the properties agree"
      (core/table-for schema [:account/name :account/status]) => :account
      "Detects the correct table name if all of the properties agree, and there is also a :db/id"
      (core/table-for schema [:db/id :account/name :account/status]) => :account
      "Remaps known graph->sql properties to ensure detection"
      (core/table-for schema [:thing/name :boo/blah]) => :sql_table
      "Throws an exception if a distinct table name cannot be resolved"
      (core/table-for schema [:db/id :account/name :user/status]) =throws=> (AssertionError #"Could not determine a single")
      "Ensures derived table names use underscores instead of hypens"
      (core/table-for schema [:a-thing/name]) => :a_thing
      (core/table-for schema [:the-thing/boo]) => :the_table
      (core/table-for schema [:the-crazy-woods/a]) => :the_crazy_woods
      "Column remaps are done before table detection"
      (core/table-for test-schema [:person/name]) => :member
      "Joins can determine table via the join name"
      (core/table-for test-schema [:db/id {:account/members [:db/id :member/name]}]) => :account
      (core/table-for schema [{:the-crazy-woods/a []}]) => :the_crazy_woods
      (core/table-for schema [{:user/name [:value]}]) => :user)))

(def sample-schema
  {::core/graph->sql {:thing/name    :sql_table/name
                      :boo/blah      :sql_table/prop
                      :the-thing/boo :the-table/bah}
   ; FROM AN SQL PERSPECTIVE...Not Om
   ::core/pks        {:account :id :member :id :invoice :id :item :id}
   ::core/joins      {
                      :account/address [:account/address_id :address/id]
                      :account/members [:account/id :member/account_id]
                      :member/account  [:member/account_id :account/id]
                      :invoice/items   [:invoice/id :invoice_items/invoice_id :invoice_items/item_id :item/id]
                      :item/invoice    [:item/id :invoice_items/item_id :invoice_items/line_item_id :invoice/line_item_id]}})

(specification "columns-for"
  (assertions
    "Returns a set"
    (core/columns-for test-schema [:t/c]) =fn=> set?
    "Resolves the ID column via the derived table name"
    (core/columns-for test-schema [:db/id :person/name :person/account]) => #{:member/id :member/name :member/account_id})
  (assert (s/valid? ::core/schema sample-schema) "Schema is valid")
  (behavior "id-columns from schema"
    (assertions "Gives back a set of :table/col keywords that represent the SQL database ID columns"
      (core/id-columns sample-schema) => #{:account/id :member/id :invoice/id :item/id}))
  (assertions
    "Converts an Om property to a proper column selector in SQL, with as AS clause of the Om property"
    (core/columns-for sample-schema [:thing/name]) => #{:sql_table/name :sql_table/id}
    (core/columns-for sample-schema [:the-thing/boo]) => #{:the_table/bah :the_table/id}
    "Joins contribute a column if the table has a join key."
    (core/columns-for sample-schema [:account/address]) => #{:account/address_id :account/id}
    (core/columns-for test-schema [:db/id {:account/members [:db/id :member/name]}]) => #{:account/id}
    "Joins without a local join column contribute their PK instead"
    (core/columns-for sample-schema [:account/members]) => #{:account/id}
    (core/columns-for sample-schema [:invoice/items]) => #{:invoice/id}
    (core/columns-for sample-schema [:item/invoice]) => #{:item/id}))

(specification "Column Specification: `column-spec`"
  (assertions
    "Translates an sqlprop to an SQL selector"
    (core/column-spec sample-schema :account/name) => "account.name AS \"account/name\""))

(specification "sqlprop-for-join"
  (assertions
    "pulls the ID column if it is a FK back-reference join"
    (core/sqlprop-for-join test-schema {:account/members [:db/id :member/name]}) => :account/id
    (core/sqlprop-for-join test-schema {:account/invoices [:db/id :invoice/created_on]}) => :account/id
    "Pulls the correct FK column when edge goes from the given table"
    (core/sqlprop-for-join test-schema {:member/account [:db/id :account/name]}) => :member/account_id
    (core/sqlprop-for-join test-schema {:invoice/account [:db/id :account/name]}) => :invoice/account_id))

(specification "forward? and reverse?"
  (assertions
    "forward? is true when the join FK points from the source table to the PK of the target table"
    (core/forward? test-schema {:account/members [:db/id]}) => false
    (core/forward? test-schema {:account/settings [:db/id]}) => true
    (core/forward? test-schema {:invoice/items [:db/id]}) => false
    (core/forward? test-schema :account/members) => false
    (core/forward? test-schema :account/settings) => true
    (core/forward? test-schema :invoice/items) => false
    "reverse? is true when the join FK points from the source table to the PK of the target table"
    (core/reverse? test-schema {:account/members [:db/id]}) => true
    (core/reverse? test-schema {:account/settings [:db/id]}) => false
    (core/reverse? test-schema {:invoice/items [:db/id]}) => true))

(specification "Single-level query-for query generation"
  (assertions
    "Generates a base non-recursive SQL query that includes necessary join resolution columns"
    (core/query-for test-schema nil [:db/id {:account/members [:db/id :member/name]}] (sorted-set 1 5 7 9)) => "SELECT account.id AS \"account/id\" FROM account WHERE account.id IN (1,5,7,9)"
    (core/query-for test-schema :account/members [:db/id :member/name] (sorted-set 1 5)) => "SELECT member.account_id AS \"member/account_id\",member.id AS \"member/id\",member.name AS \"member/name\" FROM member WHERE member.account_id IN (1,5)"
    (core/query-for test-schema :account/settings [:db/id :settings/auto-open?] (sorted-set 3)) => "SELECT settings.auto_open AS \"settings/auto_open\",settings.id AS \"settings/id\" FROM settings WHERE settings.id IN (3)"
    (core/query-for test-schema nil [:db/id :boo/name :boo/bah] #{3}) => "SELECT boo.bah AS \"boo/bah\",boo.id AS \"boo/id\",boo.name AS \"boo/name\" FROM boo WHERE boo.id IN (3)"
    "Derives correct SQL table name if possible"
    (core/query-for test-schema nil [:db/id {:account/members [:db/id :member/name]}] (sorted-set 1 5 7 9)) => "SELECT account.id AS \"account/id\" FROM account WHERE account.id IN (1,5,7,9)"
    (core/query-for test-schema nil [:db/id] (sorted-set 1 5 7 9)) =throws=> (AssertionError #"Could not determine")))

(def test-rows [; basic to-one and to-many
                (core/seed-row :settings {:id :id/joe-settings :auto_open true :keyboard_shortcuts false})
                (core/seed-row :settings {:id :id/mary-settings :auto_open false :keyboard_shortcuts true})
                (core/seed-row :account {:id :id/joe :name "Joe" :settings_id :id/joe-settings})
                (core/seed-row :account {:id :id/mary :name "Mary" :settings_id :id/mary-settings})
                (core/seed-row :member {:id :id/sam :name "Sam" :account_id :id/joe})
                (core/seed-row :member {:id :id/sally :name "Sally" :account_id :id/joe})
                (core/seed-row :member {:id :id/judy :name "Judy" :account_id :id/mary})

                ; many-to-many
                (core/seed-row :invoice {:id :id/invoice-1 :account_id :id/joe :invoice_date (tm/date-time 2017 03 04)})
                (core/seed-row :invoice {:id :id/invoice-2 :account_id :id/joe :invoice_date (tm/date-time 2016 01 02)})
                (core/seed-row :item {:id :id/gadget :name "gadget"})
                (core/seed-row :item {:id :id/widget :name "widget"})
                (core/seed-row :item {:id :id/spanner :name "spanner"})
                (core/seed-row :invoice_items {:id :join-row-1 :invoice_id :id/invoice-1 :item_id :id/gadget :invoice_items/quantity 2})
                (core/seed-row :invoice_items {:id :join-row-2 :invoice_id :id/invoice-2 :item_id :id/widget :invoice_items/quantity 8})
                (core/seed-row :invoice_items {:id :join-row-3 :invoice_id :id/invoice-2 :item_id :id/spanner :invoice_items/quantity 1})
                (core/seed-row :invoice_items {:id :join-row-4 :invoice_id :id/invoice-2 :item_id :id/gadget :invoice_items/quantity 5})

                ; graph loop
                (core/seed-update :account :id/joe {:spouse_id :id/mary})
                (core/seed-update :account :id/mary {:spouse_id :id/joe})

                ; non-looping recursion with some depth
                (core/seed-row :todo_list {:id :list-1 :name "Things to do"})
                (core/seed-row :todo_list_item {:id :item-1 :label "A" :todo_list_id :list-1})
                (core/seed-row :todo_list_item {:id :item-1-1 :label "A.1" :parent_item_id :item-1})
                (core/seed-row :todo_list_item {:id :item-1-1-1 :label "A.1.1" :parent_item_id :item-1-1})
                (core/seed-row :todo_list_item {:id :item-2 :label "B" :todo_list_id :list-1})
                (core/seed-row :todo_list_item {:id :item-2-1 :label "B.1" :parent_item_id :item-2})
                (core/seed-row :todo_list_item {:id :item-2-2 :label "B.2" :parent_item_id :item-2})])

(specification "Integration Tests for Graph Queries (PostgreSQL)" :integration
  (with-database [db test-database]
    (let [{:keys [id/joe id/mary id/invoice-1 id/invoice-2 id/gadget id/widget id/spanner id/sam id/sally id/judy id/joe-settings
                  id/mary-settings list-1 item-1 item-1-1 item-1-1-1 item-2 item-2-1 item-2-2]} (core/seed! db test-schema test-rows)
          query                       [:db/id :account/name {:account/invoices [:db/id
                                                                                ;{:invoice/invoice_items [:invoice_items/quantity]}
                                                                                {:invoice/items [:db/id :item/name]}]}]
          expected-result             {:db/id            joe
                                       :account/name     "Joe"
                                       :account/invoices [{:db/id invoice-1 :invoice/items [{:db/id gadget :item/name "gadget"}]}
                                                          {:db/id invoice-2 :invoice/items [{:db/id gadget :item/name "gadget"}
                                                                                            {:db/id widget :item/name "widget"}
                                                                                            {:db/id spanner :item/name "spanner"}]}]}
          query-2                     [:db/id :account/name {:account/members [:db/id :person/name]} {:account/settings [:db/id :settings/auto-open?]}]
          expected-result-2           [{:db/id            joe
                                        :account/name     "Joe"
                                        :account/members  [{:db/id sam :person/name "Sam"}
                                                           {:db/id sally :person/name "Sally"}]
                                        :account/settings {:db/id joe-settings :settings/auto-open? true}}
                                       {:db/id            mary
                                        :account/name     "Mary"
                                        :account/settings {:db/id mary-settings :settings/auto-open? false}
                                        :account/members  [{:db/id judy :person/name "Judy"}]}]
          query-3                     [:db/id :item/name {:item/invoices [:db/id {:invoice/account [:db/id :account/name]}]}]
          expected-result-3           [{:db/id         gadget :item/name "gadget"
                                        :item/invoices [{:db/id invoice-1 :invoice/account {:db/id joe :account/name "Joe"}}
                                                        {:db/id invoice-2 :invoice/account {:db/id joe :account/name "Joe"}}]}]
          root-set                    #{joe}
          recursive-query             '[:db/id :todo-list/name {:todo-list/items [:db/id :todo-list-item/label {:todo-list-item/subitems ...}]}]
          recursive-query-depth       '[:db/id :todo-list/name {:todo-list/items [:db/id :todo-list-item/label {:todo-list-item/subitems 1}]}]
          recursive-query-loop        '[:db/id :account/name {:account/spouse ...}]
          recursive-expectation       [{:db/id list-1 :todo-list/name "Things to do" :todo-list/items
                                               [{:db/id                   item-1 :todo-list-item/label "A"
                                                 :todo-list-item/subitems [{:db/id                   item-1-1 :todo-list-item/label "A.1"
                                                                            :todo-list-item/subitems [{:db/id item-1-1-1 :todo-list-item/label "A.1.1"}]}]}
                                                {:db/id                   item-2 :todo-list-item/label "B"
                                                 :todo-list-item/subitems [{:db/id item-2-1 :todo-list-item/label "B.1"} {:db/id item-2-2 :todo-list-item/label "B.2"}]}]}]
          recursive-expectation-depth [{:db/id list-1 :todo-list/name "Things to do" :todo-list/items
                                               [{:db/id                   item-1 :todo-list-item/label "A"
                                                 :todo-list-item/subitems [{:db/id item-1-1 :todo-list-item/label "A.1"}]}
                                                {:db/id                   item-2 :todo-list-item/label "B"
                                                 :todo-list-item/subitems [{:db/id item-2-1 :todo-list-item/label "B.1"} {:db/id item-2-2 :todo-list-item/label "B.2"}]}]}]
          recursive-expectation-loop  [{:db/id          joe :account/name "Joe"
                                        :account/spouse {:db/id          mary :account/name "Mary"
                                                         :account/spouse {:db/id joe :account/name "Joe"}}}]
          source-table                :account]
      (assertions
        "to-many"
        (core/run-query db test-schema :account/id query #{joe}) => [expected-result]
        "parallel subjoins"
        (core/run-query db test-schema :account/id query-2 (sorted-set joe mary)) => expected-result-2
        "recursion"
        (core/run-query db test-schema :todo-list/id recursive-query (sorted-set list-1)) => recursive-expectation
        "recursion with depth limit"
        (core/run-query db test-schema :todo-list/id recursive-query-depth (sorted-set list-1)) => recursive-expectation-depth
        "recursive loop detection"
        (core/run-query db test-schema :account/id recursive-query-loop (sorted-set joe)) => recursive-expectation-loop
        "reverse many-to-many"
        (core/run-query db test-schema :account/id query-3 (sorted-set gadget)) => expected-result-3))))

(specification "MySQL Integration Tests" :mysql
  (with-database [db mysql-database]
    (let [{:keys [id/joe id/mary id/invoice-1 id/invoice-2 id/gadget id/widget id/spanner id/sam id/sally id/judy
                  id/joe-settings id/mary-settings]} (core/seed! db mysql-schema test-rows)
          query             [:db/id :account/name {:account/invoices [:db/id
                                                                      ; TODO: data on join table
                                                                      ;{:invoice/invoice_items [:invoice_items/quantity]}
                                                                      {:invoice/items [:db/id :item/name]}]}]
          expected-result   {:db/id            joe
                             :account/name     "Joe"
                             :account/invoices [{:db/id invoice-1 :invoice/items [{:db/id gadget :item/name "gadget"}]}
                                                {:db/id invoice-2 :invoice/items [{:db/id widget :item/name "widget"}
                                                                                  {:db/id spanner :item/name "spanner"}
                                                                                  {:db/id gadget :item/name "gadget"}]}]}
          query-2           [:db/id :account/name {:account/members [:db/id :person/name]} {:account/settings [:db/id :settings/auto-open?]}]
          expected-result-2 [{:db/id            joe
                              :account/name     "Joe"
                              :account/members  [{:db/id sam :person/name "Sam"}
                                                 {:db/id sally :person/name "Sally"}]
                              :account/settings {:db/id joe-settings :settings/auto-open? true}}
                             {:db/id           mary
                              :account/name    "Mary"
                              :account/settings {:db/id mary-settings :settings/auto-open? false}
                              :account/members [{:db/id judy :person/name "Judy"}]}]
          query-3           [:db/id :item/name {:item/invoices [:db/id {:invoice/account [:db/id :account/name]}]}]
          expected-result-3 [{:db/id         gadget :item/name "gadget"
                              :item/invoices [{:db/id invoice-1 :invoice/account {:db/id joe :account/name "Joe"}}
                                              {:db/id invoice-2 :invoice/account {:db/id joe :account/name "Joe"}}]}]
          root-set          #{joe}
          source-table      :account
          fix-nums          (fn [result]
                              (clojure.walk/postwalk
                                (fn [ele]
                                  (if (= java.math.BigInteger (type ele))
                                    (long ele)
                                    ele)) result))]
      (assertions
        "many-to-many (forward)"
        (fix-nums (core/run-query db mysql-schema :account/id query #{joe})) => (fix-nums [expected-result])
        "one-to-many query (forward)"
        (fix-nums (core/run-query db mysql-schema :account/id query-2 (sorted-set joe mary))) => (fix-nums expected-result-2)
        "many-to-many (reverse)"
        (core/run-query db mysql-schema :account/id query-3 (sorted-set gadget)) => expected-result-3))))

(specification "H2 Integration Tests" :h2
  (with-database [db h2-database]
    (let [{:keys [id/joe id/mary id/invoice-1 id/invoice-2 id/gadget id/widget id/spanner id/sam
                  id/sally id/judy id/joe-settings id/mary-settings]} (core/seed! db h2-schema test-rows)
          query             [:db/id :account/name {:account/invoices [:db/id
                                                                      ; TODO: data on join table
                                                                      ;{:invoice/invoice_items [:invoice_items/quantity]}
                                                                      {:invoice/items [:db/id :item/name]}]}]
          expected-result   {:db/id            joe
                             :account/name     "Joe"
                             :account/invoices [{:db/id invoice-1 :invoice/items [{:db/id gadget :item/name "gadget"}]}
                                                {:db/id invoice-2 :invoice/items [{:db/id widget :item/name "widget"}
                                                                                  {:db/id spanner :item/name "spanner"}
                                                                                  {:db/id gadget :item/name "gadget"}]}]}
          query-2           [:db/id :account/name {:account/members [:db/id :person/name]} {:account/settings [:db/id :settings/auto-open?]}]
          expected-result-2 [{:db/id            joe
                              :account/name     "Joe"
                              :account/members  [{:db/id sam :person/name "Sam"}
                                                 {:db/id sally :person/name "Sally"}]
                              :account/settings {:db/id joe-settings :settings/auto-open? true}}
                             {:db/id           mary
                              :account/name    "Mary"
                              :account/settings {:db/id mary-settings :settings/auto-open? false}
                              :account/members [{:db/id judy :person/name "Judy"}]}]
          query-3           [:db/id :item/name {:item/invoices [:db/id {:invoice/account [:db/id :account/name]}]}]
          expected-result-3 [{:db/id         gadget :item/name "gadget"
                              :item/invoices [{:db/id invoice-1 :invoice/account {:db/id joe :account/name "Joe"}}
                                              {:db/id invoice-2 :invoice/account {:db/id joe :account/name "Joe"}}]}]
          root-set          #{joe}
          source-table      :account
          fix-nums          (fn [result]
                              (clojure.walk/postwalk
                                (fn [ele]
                                  (if (= java.math.BigInteger (type ele))
                                    (long ele)
                                    ele)) result))]
      (assertions
        "many-to-many (forward)"
        (core/run-query db h2-schema :account/id query #{joe}) => [expected-result]
        "one-to-many query (forward)"
        (core/run-query db h2-schema :account/id query-2 (sorted-set joe mary)) => expected-result-2
        "many-to-many (reverse)"
        (core/run-query db h2-schema :account/id query-3 (sorted-set gadget)) => expected-result-3))))


(comment
  (do
    (require 'taoensso.timbre)
    (taoensso.timbre/set-level! :error)))

