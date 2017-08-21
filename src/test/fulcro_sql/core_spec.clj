(ns fulcro-sql.core-spec
  (:require
    [fulcro-spec.core :refer [assertions specification]]
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
  - Root set is a data structure with the shape {:rows [] :row-ids #{}}
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
   :omprop->dbprop   {:db/id :id}

   ; A second layer of mapping. This gets whole groups of props for renaming
   ; the namespace to the proper table (map of symbol to symbol).
   :namespace->table {'person   'user
                      'settings 'account}

   ; Joins are configured with respect to the SQL database, *not* the Om
   ; props.
   :joins            {
                      :user/account_id {:type         :one-to-one
                                        :target-table :account
                                        :join-from    :user/account_id
                                        }

                      }

   })

(specification "Root Set"
  (assertions
    1 => 1))
