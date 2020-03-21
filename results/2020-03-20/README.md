These benchmarks were run using Go version 1.14.1 with the latest released versions of all libraries.

The hosts were Digital Ocean CPU-Optimized 4 GB / 2 CPUs in the same data center.

The tests were run under 3 conditions:

1. PostgreSQL and benchmark application on same host connected with Unix socket.
2. PostgreSQL and benchmark application on separate hosts connected over TCP with TLS encryption.
3. PostgreSQL and benchmark application on separate hosts connected over TCP without TLS encryption.

Remote TCP appears to add about 120µs of latency on top beyond a Unix socket on the local host. TLS adds an addition ~15% of latency.
