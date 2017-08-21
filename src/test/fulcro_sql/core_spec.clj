(ns fulcro-sql.core-spec
  (:require
    [fulcro-spec.core :refer [assertions specification behavior when-mocking]]
    [fulcro-sql.core :as core]))

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

(def schema
  {
   ; top-level transform from Om to SQL naming
   ; convention is :TABLE/COLUMN as a keyword. Hypens auto-map to underscores.
   ; All other special characters are dropped. e.g. :user/is-ok? becomes :user/is_ok
   ; More complex mapping must be manually done via this configuration:
   :om->sql {:db/id :id}

   ; Joins are configured with respect to the SQL database, *not* the Om
   ; props.
   :joins   {
             :user/account_id {:type         :one-to-one
                               :target-table :account
                               :join-from    :user/account_id
                               }

             }

   })

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
      (core/table-for schema [:the-crazy-woods/a]) => :the_crazy_woods)))


(specification "Column Specification"
  (let [schema      {:om->sql {:thing/name    :sql_table/name
                               :boo/blah      :sql_table/prop
                               :the-thing/boo :the-table/bah}}
        table-alias "a1"]
    (assertions
      "Converts an Om property to a proper column selector in SQL, with as AS clause of the Om property"
      (core/column-spec table-alias schema :account/name) => "a1.name AS \":account/name\""
      (core/column-spec table-alias schema :thing/name) => "a1.name AS \":thing/name\""
      (core/column-spec table-alias schema :the-thing/boo) => "a1.bah AS \":the-thing/boo\"")))

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
