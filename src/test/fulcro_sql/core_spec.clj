(ns fulcro-sql.core-spec
  (:require
    [fulcro-spec.core :refer [assertions specification behavior when-mocking component]]
    [fulcro-sql.core :as core]
    [fulcro-sql.test-helpers :refer [with-database]]
    [clojure.spec.alpha :as s]
    [com.stuartsierra.component :as component]
    [clojure.java.jdbc :as jdbc]
    [taoensso.timbre :as timbre]))

(def test-database {:hikaricp-config "test.properties"
                    :migrations      ["classpath:migrations/test"]})

(def test-schema {::core/om->sql {:person/name    :member/name
                                  :person/account :member/account_id}
                  ::core/joins   {:account/members [:account/id :member/account_id]
                                  :member/account  [:member/account_id :account/id]}
                  ::core/pks     {:account :id
                                  :member  :id}})

(specification "Database Component" :integration
  (behavior "Can create a functional database pool from HikariCP properties and Flyway migrations."
    (with-database [db test-database]
      (let [result (jdbc/insert! db :account {:name "Tony"})
            row    (first result)]
        (assertions
          (:name row) => "Tony")))))

(specification "next-id" :integration
  (behavior "Pulls a monotonically increasing ID from the database"
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
        (:last_edited_by real-joe) => sam))))




(specification "Table Detection: `table-for`"
  (let [schema {::core/pks     {}
                ::core/joins   {}
                ::core/om->sql {:thing/name    :sql_table/name
                                :boo/blah      :sql_table/prop
                                :the-thing/boo :the-table/bah}}]
    (assertions
      ":id and :db/id are ignored"
      (core/table-for schema [:account/name :db/id :id]) => :account
      "Detects the correct table name if all of the properties agree"
      (core/table-for schema [:account/name :account/status]) => :account
      "Detects the correct table name if all of the properties agree, and there is also a :db/id"
      (core/table-for schema [:db/id :account/name :account/status]) => :account
      "Remaps known om->sql properties to ensure detection"
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
  {::core/om->sql {:thing/name    :sql_table/name
                   :boo/blah      :sql_table/prop
                   :the-thing/boo :the-table/bah}
   ; FROM AN SQL PERSPECTIVE...Not Om
   ::core/pks     {:account :id :member :id :invoice :id :item :id}
   ::core/joins   {
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

(specification "Single-level query-for query generation"
  (assertions
    "Generates a base non-recursive SQL query"
    (core/query-for test-schema :account [:db/id {:account/members [:db/id :member/name]}] #{1 5 7 9}) => "SELECT account.id AS \"account/id\" FROM account WHERE account.id IN (7,1,9,5)"
    "Derives table name if none is supplied when possible"
    (core/query-for test-schema nil [:db/id {:account/members [:db/id :member/name]}] #{1 5 7 9}) => "SELECT account.id AS \"account/id\" FROM account WHERE account.id IN (7,1,9,5)"
    (core/query-for test-schema nil [:db/id] #{1 5 7 9}) =throws=> (AssertionError #"Could not determine")
    "Refuses to generate a query if the specified table does not agree with the query"
    (core/query-for test-schema :member [:db/id {:account/members [:db/id :member/name]}] #{1 5 7 9}) =throws=> (java.lang.AssertionError #"Target.*mismatch")
    "Properly generates a comma-separated list of selectors"
    (core/query-for test-schema nil [:db/id :boo/name :boo/bah] #{3}) => "SELECT boo.name AS \"boo/name\",boo.id AS \"boo/id\",boo.bah AS \"boo/bah\" FROM boo WHERE boo.id IN (3)"))

(specification "target-table-for-join"
  (assertions
    "finds the table name where data will come from for a join"
    (core/target-table-for-join sample-schema :invoice/items) => :item
    (core/target-table-for-join sample-schema :account/members) => :member
    "Returns nil when the columns is not a registered join"
    (core/target-table-for-join sample-schema :account/id) => nil))

(specification "query-for-join" :focused
  (assertions
    "Can follow a standard one-to-many where the FK is on the target table."
    (core/query-for-join test-schema {:account/members [:db/id :member/name]} [{:account/id 1} {:account/id 2}])
    => "SELECT member.name AS \"member/name\",member.id AS \"member/id\" FROM member WHERE member.account_id IN (1,2)"
    "Can follow a standard one-to-one where the FK is on the source table to the target ID."
    (core/query-for-join sample-schema {:account/address [:db/id :address/street]} [{:account/address_id 8} {:account/address_id 11}])
    => "SELECT address.id AS \"address/id\",address.street AS \"address/street\" FROM address WHERE address.id IN (11,8)"

    ;:invoice/items   [:invoice/id :invoice_items/invoice_id :invoice_items/item_id :item/id]
    "Can follow a standard many-to-many which will include the source table ID for join-resolution"
    (core/query-for-join sample-schema {:invoice/items [:db/id :item/amount]} [{:invoice/id 3} {:invoice/id 5}])
    => "SELECT invoice_items.invoice_id AS \"invoice_items/invoice_id\",item.id AS \"item/id\",item.amount AS \"item/amount\" FROM invoice_items INNER JOIN item ON invoice_items.item_id = item.id WHERE invoice_items.invoice_id IN (3,5)"
    ))

