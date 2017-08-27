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
    [taoensso.timbre :as timbre]))

(def test-database {:hikaricp-config "test.properties"
                    :migrations      ["classpath:migrations/test"]})

(def test-schema {::core/graph->sql {:person/name                  :member/name
                                     :person/account               :member/account_id
                                     :settings/auto-open?          :settings/auto_open
                                     :settings/keyboard-shortcuts? :settings/keyboard_shortcuts}
                  ::core/joins      {:account/members  (core/to-many [:account/id :member/account_id])
                                     :account/settings (core/to-one [:account/settings_id :settings/id])
                                     :member/account   (core/to-one [:member/account_id :account/id])
                                     :account/invoices (core/to-many [:account/id :invoice/account_id])
                                     :invoice/account  (core/to-one [:invoice/account_id :account/id])
                                     :invoice/items    (core/to-many [:invoice/id :invoice_items/invoice_id :invoice_items/item_id :item/id])
                                     :item/invoices    (core/to-many [:item/id :invoice_items/item_id :invoice_items/invoice_id :invoice/id])}
                  ::core/pks        {}})

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

(specification "Single-level query-for query generation" :focused
  (assertions
    "Generates a base non-recursive SQL query that includes necessary join resolution columns"
    (core/query-for test-schema nil [:db/id {:account/members [:db/id :member/name]}] (sorted-set 1 5 7 9)) => "SELECT account.id AS \"account/id\" FROM account WHERE account.id IN (1,5,7,9)"
    (core/query-for test-schema :account/members [:db/id :member/name] (sorted-set 1 5)) => "SELECT member.account_id AS \"member/account_id\",member.id AS \"member/id\",member.name AS \"member/name\" FROM member WHERE member.account_id IN (1,5)"
    (core/query-for test-schema :account/settings [:db/id :settings/auto-open?] (sorted-set 3)) => "SELECT settings.auto_open AS \"settings/auto_open\",settings.id AS \"settings/id\" FROM settings WHERE settings.id IN (3)"
    (core/query-for test-schema nil [:db/id :boo/name :boo/bah] #{3}) => "SELECT boo.bah AS \"boo/bah\",boo.id AS \"boo/id\",boo.name AS \"boo/name\" FROM boo WHERE boo.id IN (3)"
    "Derives correct SQL table name if possible"
    (core/query-for test-schema nil [:db/id {:account/members [:db/id :member/name]}] (sorted-set 1 5 7 9)) => "SELECT account.id AS \"account/id\" FROM account WHERE account.id IN (1,5,7,9)"
    (core/query-for test-schema nil [:db/id] (sorted-set 1 5 7 9)) =throws=> (AssertionError #"Could not determine")))

(specification "target-table-for-join"
  (assertions
    "finds the table name where data will come from for a join"
    (core/target-table-for-join sample-schema :invoice/items) => :item
    (core/target-table-for-join sample-schema :account/members) => :member
    "Returns nil when the columns is not a registered join"
    (core/target-table-for-join sample-schema :account/id) => nil))

#_(specification "query-for-join"
    (assertions
      "Can follow a standard one-to-many where the FK is on the target table."
      (first (core/query-for-join test-schema {:account/members [:db/id :member/name]} [{:account/id 1} {:account/id 2}]))
      => "SELECT member.account_id AS \"member/account_id\",member.name AS \"member/name\",member.id AS \"member/id\" FROM member WHERE member.account_id IN (1,2)"
      "Can follow a standard one-to-one where the FK is on the source table to the target ID."
      (first (core/query-for-join sample-schema {:account/address [:db/id :address/street]} [{:account/address_id 8} {:account/address_id 11}]))
      => "SELECT address.id AS \"address/id\",address.street AS \"address/street\" FROM address WHERE address.id IN (11,8)"

      ;:invoice/items   [:invoice/id :invoice_items/invoice_id :invoice_items/item_id :item/id]
      "Can follow a standard many-to-many which will include the source table ID for join-resolution"
      (first (core/query-for-join sample-schema {:invoice/items [:db/id :item/amount]} [{:invoice/id 3} {:invoice/id 5}]))
      => "SELECT invoice_items.invoice_id AS \"invoice_items/invoice_id\",item.id AS \"item/id\",item.amount AS \"item/amount\" FROM invoice_items INNER JOIN item ON invoice_items.item_id = item.id WHERE invoice_items.invoice_id IN (3,5)"))

(def pretend-results
  {:account       [{:account/id :id/joe :account/name "Joe"}]
   :invoice       [{:invoice/id :id/invoice-1 :invoice/account_id :id/joe :invoice/invoice_date (tm/date-time 2017 03 04)}
                   {:invoice/id :id/invoice-2 :invoice/account_id :id/joe :invoice/invoice_date (tm/date-time 2016 01 02)}]
   :item          [{:item/id :id/gadget :item/name "gadget"}
                   {:item/id :id/widget :item/name "widget"}
                   {:item/id :id/spanner :item/name "spanner"}]
   :invoice_items [{:invoice_items/id :join-row-1 :invoice_items/invoice_id :id/invoice-1 :invoice_items/item_id :id/gadget :invoice_items/quantity 2}
                   {:invoice_items/id :join-row-2 :invoice_items/invoice_id :id/invoice-2 :invoice_items/item_id :id/widget :invoice_items/quantity 8}
                   {:invoice_items/id :join-row-3 :invoice_items/invoice_id :id/invoice-2 :invoice_items/item_id :id/spanner :invoice_items/quantity 1}
                   {:invoice_items/id :join-row-4 :invoice_items/invoice_id :id/invoice-2 :invoice_items/item_id :id/gadget :invoice_items/quantity 5}]})

(def test-rows [(core/seed-row :settings {:id :id/joe-settings :auto_open true :keyboard_shortcuts false})
                (core/seed-row :account {:id :id/joe :name "Joe" :settings_id :id/joe-settings})
                (core/seed-row :account {:id :id/mary :name "Mary"})
                (core/seed-row :member {:id :id/sam :name "Sam" :account_id :id/joe})
                (core/seed-row :member {:id :id/sally :name "Sally" :account_id :id/joe})
                (core/seed-row :member {:id :id/judy :name "Judy" :account_id :id/mary})
                (core/seed-row :invoice {:id :id/invoice-1 :account_id :id/joe :invoice_date (tm/date-time 2017 03 04)})
                (core/seed-row :invoice {:id :id/invoice-2 :account_id :id/joe :invoice_date (tm/date-time 2016 01 02)})
                (core/seed-row :item {:id :id/gadget :name "gadget"})
                (core/seed-row :item {:id :id/widget :name "widget"})
                (core/seed-row :item {:id :id/spanner :name "spanner"})
                (core/seed-row :invoice_items {:id :join-row-1 :invoice_id :id/invoice-1 :item_id :id/gadget :invoice_items/quantity 2})
                (core/seed-row :invoice_items {:id :join-row-2 :invoice_id :id/invoice-2 :item_id :id/widget :invoice_items/quantity 8})
                (core/seed-row :invoice_items {:id :join-row-3 :invoice_id :id/invoice-2 :item_id :id/spanner :invoice_items/quantity 1})
                (core/seed-row :invoice_items {:id :join-row-4 :invoice_id :id/invoice-2 :item_id :id/gadget :invoice_items/quantity 5})])

(specification "Integration Tests for Graph Queries" :integration :focused
  (with-database [db test-database]
    (let [{:keys [id/joe id/mary id/invoice-1 id/invoice-2 id/gadget id/widget id/spanner id/sam id/sally id/judy id/joe-settings]} (core/seed! db test-schema test-rows)
          query             [:db/id :account/name {:account/invoices [:db/id
                                                                      ;{:invoice/invoice_items [:invoice_items/quantity]}
                                                                      {:invoice/items [:db/id :item/name]}]}]
          expected-result   {:account/id       joe
                             :account/name     "Joe"
                             :account/invoices [{:invoice/id invoice-1 :invoice/items [{:item/id gadget :item/name "gadget"}]}
                                                {:invoice/id invoice-2 :invoice/items [{:item/id widget :item/name "widget"}
                                                                                       {:item/id spanner :item/name "spanner"}
                                                                                       {:item/id gadget :item/name "gadget"}]}]}
          query-2           [:db/id :account/name {:account/members [:db/id :person/name]} {:account/settings [:db/id :settings/auto-open?]}]
          expected-result-2 [{:db/id            joe
                              :account/name     "Joe"
                              :account/members  [{:db/id sam :person/name "Sam"}
                                                 {:db/id sally :person/name "Sally"}]
                              :account/settings {:db/id joe-settings :settings/auto-open? true}}
                             {:db/id            mary
                              :account/name     "Mary"
                              :account/settings {}
                              :account/members  [{:db/id judy :person/name "Judy"}]}]
          root-set          #{joe}
          source-table      :account]
      (assertions
        (core/run-query db test-schema :account/id query #{joe}) => [expected-result]
        (core/run-query db test-schema :account/id query-2 (sorted-set joe mary)) => expected-result-2))))

(comment
  (do
    (require 'taoensso.timbre)
    (taoensso.timbre/set-level! :error)))
