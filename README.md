# Go Database Benchmark

This tests the performance of [pgx native](https://github.com/jackc/pgx), [pgx through
database/sql](https://github.com/jackc/pgx/tree/master/stdlib), [pq](https://github.com/lib/pq) through database/sql,
[go-pg](https://github.com/go-pg/pg), and theoretical maximum PostgreSQL performance. Unless the test specifically
states otherwise it always uses prepared statements.

## Interesting Results

* Network latency and PostgreSQL server processing time dominate most real world tests.
* A simple query executed locally via Unix domain sockets can be several times faster than over the network.
* TLS is cheap but it is not free (~15% impact).

All the Go drivers perform similarly when performing queries that return small result sets over TCP. Larger differences are apparent with large result sets especially if one driver uses the binary format and another uses the text format for results.

In addition, using driver-specific features can yield significant performance deltas. For example:

* When network latency is the dominant factor using batch operations makes a huge difference ([pgx](https://github.com/jackc/pgx) and [go-pg](https://github.com/go-pg/pg) support in various ways).
* [pgx](https://github.com/jackc/pgx) automatically prepares and caches SQL. This can make a large difference for code that does not explicitly prepare statements, but has no advantage if it does.
* [go-pg](https://github.com/go-pg/pg) is an ORM as well as a driver. Idiomatic usage does more work than other drivers. This makes direct comparison difficult.

The raw results analyzed above are in the results directory. You can also run the benchmarks for yourself in your own environment.

## Configuration

go_db_bench reads its configuration from the standard PostgreSQL environment variables such as `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, and `PGSSLMODE`.

## Core Benchmarks

go_db_bench includes tests selecting one value, one row, and multiple rows.

Example execution:

    PGHOST=/private/tmp go test -test.bench=. -test.benchmem

## HTTP Benchmarks

go_db_bench includes a simple HTTP server that serves JSON directly from
PostgreSQL. This allows testing the performance of database drivers in a more
real-world environment.

Example execution:

    go build && PGHOST=/private/tmp ./go_db_bench

It exposes the following endpoints:

* /people/pgx-native - pgx through its native interface
* /people/pgx-stdlib - pgx through database/sql
* /people/pq - pq through database/sql

Start the server and use your favorite HTTP load tester to benchmark (I
recommend [siege](http://www.joedog.org/siege-home/) or
[overload](https://github.com/jackc/overload)).

## Theoretical Max PostgreSQL Performance

This benchmark includes a minimum PostgreSQL driver sufficient to establish a
connection and prepare statements. Query execution is benchmarked by sending a
[]byte filled with the query command and reading until the ready for query
message is received. This should be the theoretical best performance a Go
PostgreSQL driver could achieve.

Caveat: The returned data is not checked or parsed. It is only read until the
ready for query message is received. If an error occurs it may not be apparent
which could cause the timing to be misleading.
