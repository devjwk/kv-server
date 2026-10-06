# BENCHMARK.md

## Benchmark Setup

The Mini-KV server was benchmarked using the concurrent `bench_client` program. The benchmark used a 90% read and 10% write workload, with each client thread opening its own TCP connection to the server. Each thread sent one command at a time and waited for the server response before sending the next command.

Server configuration:

```bash
./kvserver 9000 8 1024 100
```

Benchmark commands:

```bash
./bench_client localhost 9000 1 10000 90
./bench_client localhost 9000 4 10000 90
./bench_client localhost 9000 16 10000 90
./bench_client localhost 9000 64 10000 90
```

## Results

| Concurrent Clients | Ops per Client | Total Requested Ops | Successful Ops | Failed Ops | Elapsed Time (s) | Throughput (ops/sec) |
|---:|---:|---:|---:|---:|---:|---:|
| 1  | 10,000 | 10,000  | 10,000  | 0 | 0.288498 | 34,662.32 |
| 4  | 10,000 | 40,000  | 40,000  | 0 | 0.149370 | 267,791.74 |
| 16 | 10,000 | 160,000 | 160,000 | 0 | 0.736729 | 217,176.16 |
| 64 | 10,000 | 640,000 | 640,000 | 0 | 3.277267 | 195,284.66 |

## Analysis

Throughput increased sharply from 1 client to 4 clients, rising from about 34.7k ops/sec to about 267.8k ops/sec. This shows that the worker pool can take advantage of concurrent client connections instead of forcing all work through one client session at a time. However, throughput did not continue to scale linearly after 4 clients. At 16 clients, throughput dropped to about 217.2k ops/sec, and at 64 clients it dropped further to about 195.3k ops/sec. This suggests that the server reaches a bottleneck after a small number of concurrent clients. The most likely causes are contention on the single table-wide reader-writer lock, TCP socket I/O overhead, and thread scheduling overhead. The 90% read workload benefits from concurrent GET operations, but PUT operations and sweeper passes still require exclusive write access, which limits scalability at higher client counts.