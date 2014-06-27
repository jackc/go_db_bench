# Go Database Benchmark

This tests the performance of pgx native, pgx through database/sql, pq through
database/sql, and theoretical maximum PostgreSQL performance.

## Theoretical Max PostgreSQL Performance

This benchmark includes a minimum PostgreSQL driver sufficient to establish a
connection and prepare statements. Query execution is benchmarked by sending a
[]byte filled with the query command and reading until the ready for query
message is received. This should be the theoretical best performance a Go
PostgreSQL driver could achieve.

Caveat: The returned data is not checked or parsed. It is only read until the
ready for query message is received. If an error occurs it may not be apparent
which could cause the timing to be misleading.
