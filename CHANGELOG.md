0.3.0
-----
- Dropped dependencies on Om Next

0.2.0
-----
- Support for efficient query filtering (filters converted to SQL WHERE)
- Support for minimum and maximum depth on filters so they can be targeted at levels of the graph query

0.1.0
-----
- Support for MySQL/MariaDB, and H2
- Fixed a number of issues with graph traversal
- Added support for recursive queries and loop detection
- Renamed a few things. See the new specs and updated readme.
- Most join types supported

0.0.1
-----
- Support for databases as com.stuartsierra components
- Migration support integration via Flyway
- Connection pooling via HikariCP
- Data seeding via abstract IDs
- Database integration testing support with `with-database`
- Works well with `clojure.java.jdbc`.
