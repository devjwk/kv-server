# Mini-KV Server

A multithreaded in-memory key-value server in C: TCP clients, a fixed worker pool, a reader-writer-locked hash table, and expiring keys.

| | |
|---|---|
| Period | May 2026 (CprE 308 Operating Systems, Project 2) |
| Team | Individual |
| Stack | C, POSIX threads, BSD sockets, Make |

## Overview

- **Problem:** a server that handles one client at a time wastes every core but one, and a server that shares a hash table between threads without care corrupts it. The project is to serve many clients at once while keeping the table correct.
- **Design:** the main thread accepts connections and pushes them onto a bounded queue. Worker threads pop connections and serve them. `GET` takes the table lock in read mode; `PUT`, `DEL` and the expiry sweeper take it in write mode. Statistics have their own mutex.

```
clients ──TCP──> accept thread ──> bounded queue ──> worker pool ──> hash table (rwlock)
                                                                          ^
                                                          sweeper thread ─┘ (removes expired keys)
```

## My role

Individual project. I wrote the server, the protocol parser, the work queue, the hash table, the TTL sweeper and the benchmark client, and ran the benchmarks.

## What I learned

- The producer-consumer pattern with one mutex and two condition variables (`not_empty`, `not_full`).
- Choosing lock granularity. One table-wide reader-writer lock is simple and safe, and the benchmark shows exactly where that simplicity costs throughput.
- Sizing a worker pool by measurement instead of by guess.
- Clean shutdown: waking blocked workers and the sweeper, then freeing every entry.
- Writing a benchmark client whose own overhead does not hide the server's behavior.

## Resources used

- Course lecture material on threads and synchronization, and the project handout
- Linux man pages for `pthread_rwlock`, `pthread_cond`, `socket` and `nanosleep`
- `nc` for manual protocol testing

## Results

90% GET / 10% PUT workload, 8 workers, 10,000 operations per client:

| Clients | Throughput (ops/s) | Failed operations |
|---:|---:|---:|
| 1 | 34,662 | 0 |
| 4 | 267,792 | 0 |
| 16 | 217,176 | 0 |
| 64 | 195,285 | 0 |

Throughput rises almost eightfold from 1 to 4 clients, then falls as writers and the sweeper contend for the single table lock. Full details are in [BENCHMARK.md](BENCHMARK.md).

## Limitations and next steps

- One table-wide lock limits scaling beyond a few clients. Per-bucket locks are the next step.
- The sweeper holds the write lock for a whole scan, briefly blocking every worker.
- No persistence: data is lost on restart.
- Benchmarks were run on a single development machine over loopback.

The sections below are the original project write-up.

## 1. Build and Run Instructions

Build the server and benchmark client:

```bash
make
```

Build only the benchmark client:

```bash
make bench
```

Clean generated files:

```bash
make clean
```

Run the server:

```bash
./kvserver <port> <num_workers> <num_buckets> [sweeper_interval_ms]
```

Example:

```bash
./kvserver 9000 8 1024 100
```

The fourth argument is optional. If `sweeper_interval_ms` is not provided, the server uses a default value of 500 milliseconds. The server validates that the port, worker count, bucket count, and sweeper interval are positive values.

You can test the server manually with `nc`:

```bash
nc localhost 9000
```

Example commands:

```text
PUT color red
GET color
DEL color
PUT temp hello 2
GET temp
STATS
QUIT
```

Expected protocol behavior:

```text
GET <key>                      -> VALUE <value> or NOT_FOUND
PUT <key> <value>              -> OK
PUT <key> <value> <ttl>        -> OK, where ttl must be a positive integer
DEL <key>                      -> OK or NOT_FOUND
STATS                          -> STATS keys=<n> hits=<n> misses=<n> puts=<n> dels=<n> active_conns=<n> uptime_s=<n>
QUIT                           -> BYE
Malformed command              -> ERROR <short-message>
```

Run the benchmark client:

```bash
./bench_client <host> <port> <num_clients> <ops_per_client> <read_pct>
```

Example:

```bash
./bench_client localhost 9000 16 10000 90
```

This runs 16 client threads, gives each thread 10,000 operations, and uses a 90% GET / 10% PUT workload.

## 2. Stage Status

Stage 1 is complete. The server supports TCP client connections, an in-memory chained hash table, line-based text commands, GET, PUT, DEL, STATS, QUIT, malformed command handling, and cleanup on shutdown. The hash table stores fixed-size key and value strings, and `kv_table_destroy()` frees all allocated entries before exit.

Stage 2 is complete. The server uses a fixed-size worker pool and a bounded FIFO work queue. The main thread accepts TCP connections and enqueues accepted connection file descriptors. Worker threads dequeue connection file descriptors, call `handle_client()`, serve each client until QUIT or disconnect, then close the socket. The work queue uses a mutex plus `not_empty` and `not_full` condition variables for producer-consumer synchronization.

Stage 3 is complete. The hash table is protected by a single table-wide `pthread_rwlock_t`. GET operations acquire the table lock in read mode, while PUT, DEL, and the TTL sweeper acquire it in write mode. Statistics are protected by a separate mutex so workers can update counters safely without using the table lock for stats-only data.

Stage 4 is complete. PUT supports an optional positive TTL in seconds. Entries with TTL store an absolute expiration timestamp, while entries without TTL never expire. GET checks expiration and returns NOT_FOUND for expired keys, counting that request as a miss. I chose not to lazily delete expired entries inside GET; actual removal is handled by the background sweeper. Exactly one sweeper thread is created at startup. It sleeps for the configured interval, acquires the table write lock, scans all buckets, removes expired entries, and exits cleanly during shutdown. STATS includes all required fields.

Bonus persistence is not implemented.

Known limitations:

- The hash table uses one table-wide reader-writer lock, so write-heavy workloads and high client counts can experience contention.
- The sweeper holds the table write lock while scanning and removing expired entries, so workers may briefly block during a sweep.
- Expired entries may remain in the table until the sweeper removes them, although GET still correctly returns NOT_FOUND for expired keys.
- Persistence is not implemented, so data does not survive a server restart.
- Benchmark results were collected on a local development environment, so absolute throughput may differ on the Coover 2061 Linux workstations.

## 3a. Lock Granularity

I used a single table-wide `pthread_rwlock_t` because it gives a simple and safe synchronization model for this project. The lock is stored once in `kv_table_t`, so the whole hash table is protected by one reader-writer lock rather than one lock per bucket or entry. GET acquires the lock in read mode, which allows multiple concurrent GET operations. PUT, DEL, and the TTL sweeper acquire the lock in write mode because they modify bucket links, entry contents, expiration metadata, or `num_keys`. The main benefit is correctness: a worker cannot follow a pointer to an entry while another thread is unlinking and freeing it. Bucket-level locking could improve concurrency when clients access different buckets, but it would require per-bucket lock initialization and destruction, more complicated sweeper logic, and separate synchronization for shared metadata such as `num_keys`. My benchmark with 8 workers and a 90% read workload increased from about 34.7k ops/sec with 1 client to about 267.8k ops/sec with 4 clients, but throughput dropped to about 217.2k at 16 clients and 195.3k at 64 clients. This shows the design is correct and benefits from moderate concurrency, but the coarse table-wide lock likely limits scalability at higher client counts.

## 3b. Worker Pool Sizing

Worker pool size controls how many client connections can be served at the same time. In this design, the main thread does not process client commands directly. It only accepts connections and pushes accepted socket file descriptors into the bounded work queue. Worker threads pop those descriptors and call `handle_client()` until the client disconnects or sends QUIT. I tested 2, 4, 8, and 16 workers using 16 concurrent clients, 10,000 operations per client, and a 90% GET / 10% PUT workload. Throughput was about 37,944 ops/sec with 2 workers, 76,273 ops/sec with 4 workers, 85,765 ops/sec with 8 workers, and 85,112 ops/sec with 16 workers. Performance improved significantly from 2 to 4 workers because too few workers left client connections waiting in the queue. The improvement from 4 to 8 workers was smaller, and adding 16 workers did not help. This suggests the server begins to plateau around 8 workers for this workload. After that point, the bottleneck is less about worker availability and more about shared resources such as the table-wide RW lock, socket I/O, condition variable wakeups, and OS scheduling overhead. Based on these measurements, 8 workers was a reasonable configuration.

## 3c. Sweeper Coordination

The sweeper runs as exactly one background thread created at server startup. It receives the configured `sweeper_interval_ms`, sleeps using `nanosleep()`, checks the global shutdown flag, and then calls `kv_remove_expired()`. The removal function acquires the same table-wide RW lock in write mode before scanning buckets and unlinking expired entries. This write lock is necessary because removing an entry changes linked-list pointers, frees memory, and decrements `num_keys`. It also prevents a race where a GET could observe an entry while the sweeper is deleting it. GET only uses a read lock, so it cannot run at the same time as the sweeper's write-locked removal pass. I chose to hold the write lock across the full sweep because the rest of the table uses one coarse-grained lock. Per-bucket locking would not give a real benefit unless GET, PUT, and DEL were also redesigned around bucket-level locks. The tradeoff is that workers can briefly block during a sweep, especially with many buckets or many expired keys. For this project scale, that tradeoff is acceptable because the design is simple, safe, easy to reason about, and the benchmark still completed 640,000 operations with 64 clients and zero failed operations.
