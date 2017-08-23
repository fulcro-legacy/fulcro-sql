(ns fulcro-sql.core-spec
  (:require
    [fulcro-spec.core :refer [assertions specification behavior when-mocking component]]
    [fulcro-sql.core :as core]
    [fulcro-sql.test-helpers :refer [with-database]]
    [clojure.spec.alpha :as s]
    [com.stuartsierra.component :as component]
    [clojure.java.jdbc :as jdbc]
    [taoensso.timbre :as timbre]))

(comment
  "
A given Om query running through the parser will
have a root property (that is either a property of a join). The
entry point of the processing will need to determine from that (and
its parameters) how to obtain the root set of entities of
interest, and what their root ID set is.

At that point, the join processing can take over. Using a configured
schema it can walk any of the following kinds of SQL Schema joins:

1. One to one: ID of foreign row is on the table of the root set
2. One to one: ID of root set row is on the foreign table
3. One to many: Root set ID will appear in a column on foreign table
4. Many to many: Root set ID will appear on a join table, which in
turn will have a column pointing to the foreign row

The scheme we'd like to use would issue the minimal number of queries
for a given graph walk.

Steps:

1. Your code generates the root set from a property key and params
  - Root set is a data structure with the shape {:rows [] :ids #{}}
2. Subquery is processed:
   - Derive what table the subquery is hitting
   - Generate the column list for all properties and join (ids) that exist
     on that table, based on the schema.
   - Issue a SELECT ... WHERE ... IN ... to obtain all of the rows related
   to the root set.
   - If any of the subquery items are joins, recurse.

"

  (timbre/set-level! :error)
  )

(specification "Table Detection: `table-for`"
  (let [schema {:om->sql {:thing/name    :sql_table/name
                          :boo/blah      :sql_table/prop
                          :the-thing/boo :the-table/bah}}]
    (assertions
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
      "Joins do not contribute to detection"
      (core/table-for schema [:account/a {:the-crazy-woods/a []}]) => :account
      (core/table-for schema [:account/a {:user/name [:value]}]) => :account)))

; one to one, and one-to-many, just specify the graph edge in either order
;[:account/address_id :address/id]
; SELECT address.id, account.address_id FROM address, account WHERE account.address_id = address.id
; SELECT member.id, account.id FROM account, member WHERE account.id = member.account_id AND account.id IN (1, 2, 3)
; join table involved? Just specify the path from end-to-end
;[:invoice/line_item_id :invoice_items/line_item_id :invoice_items/item_id :item/id]

(specification "Column Specification: `column-spec`"
  (let [schema {::core/om->sql {:thing/name    :sql_table/name
                                :boo/blah      :sql_table/prop
                                :the-thing/boo :the-table/bah}
                ; FROM AN SQL PERSPECTIVE...Not Om
                ::core/pks     {:account :id :member :id :invoice :id :item :id}
                ::core/joins   {
                                :account/address [:account/address_id :address/id]
                                :account/members [:account/id :member/account_id]
                                :member/account  [:member/account_id :account/id]
                                :invoice/items   [:invoice/id :invoice_items/invoice_id :invoice_items/item_id :item/id]
                                :item/invoice    [:item/id :invoice_items/item_id :invoice_items/line_item_id :invoice/line_item_id]
                                }}]
    (assert (s/valid? ::core/schema schema) "Schema is valid")
    (behavior "id-columns from schema"
      (assertions "Gives back a set of :table/col keywords that represent the SQL database ID columns"
        (core/id-columns schema) => #{:account/id :member/id :invoice/id :item/id}))
    (assertions
      "Converts an Om property to a proper column selector in SQL, with as AS clause of the Om property"
      (core/column-spec schema :account/name) => "account.name AS \"account/name\""
      (core/column-spec schema :thing/name) => "sql_table.name AS \"thing/name\""
      (core/column-spec schema :the-thing/boo) => "the_table.bah AS \"the-thing/boo\""
      "Joins contribute a column if the table has a join key."
      (core/column-spec schema :account/address) => "account.address_id AS \"account/address\""
      "Joins without a local join column contribute their PK instead"
      (core/column-spec schema :account/members) => "account.id AS \"account/members\""
      (core/column-spec schema :invoice/items) => "invoice.id AS \"invoice/items\""
      (core/column-spec schema :item/invoice) => "item.id AS \"item/invoice\"")))

(def test-database {:hikaricp-config "test.properties"
                    :migrations      ["classpath:migrations/test"]})

(def test-schema {::core/om->sql {}
                  ::core/joins   {:account/members [:account/id :member/account_id]}
                  ::core/pks     {:account :id
                                  :member  :id}})

(specification "Database Component" :integration
  (behavior "Can create a functional database pool from HikariCP properties and Flyway migrations."
    (with-database [db test-database]
      (let [result (jdbc/insert! db :account {:name "Tony"})
            row    (first result)]
        (assertions
          (:name row) => "Tony")))))

(specification "next-id"
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

(specification "seed!"
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

(comment
  (specification "Om Property-only Query"
    (behavior "Given a root set of IDs and an Om query for properties (no joins)"
      (let [root-set {:rows [{:id 1 :name "Joe" :age 42}] :ids #{1}}
            subquery [:db/id :account/status :account/created]
            schema   {}]
        (assertions
          1 =>
          "SELECT account.id as \":db/id\", account.status as \":account/status\",
          account.created as \":account/created\"
          FROM account WHERE account.user_id IN (1)")))))
