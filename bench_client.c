/*
 * bench_client.c -- Mini-KV concurrent benchmark client
 *
 * Project 2, CprE 3080, Spring 2026
 *
 * YOU write this file. The scaffolding here is intentionally minimal:
 * an argument parser and nothing else. Your job is to fill in the rest.
 *
 * Usage:
 *   ./bench_client <host> <port> <num_clients> <ops_per_client> <read_pct>
 *
 * Requirements (from the spec):
 *   - Spawn <num_clients> threads.
 *   - Each thread opens its own TCP connection to <host>:<port>.
 *   - Each thread issues <ops_per_client> operations.
 *   - <read_pct> percent of ops are GETs; the rest are PUTs.
 *   - Keys drawn from a small pool (~1000 keys) so GETs actually hit.
 *   - Report total wall-clock time and overall throughput (ops/sec).
 *
 * Hints:
 *   - Use clock_gettime(CLOCK_MONOTONIC, ...) to measure elapsed time.
 *   - Each thread needs its own rand_r() seed to avoid serialization on
 *     the global rand() lock.
 *   - Read the server's reply line-by-line; don't assume one TCP packet
 *     per command.
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define KEY_POOL_SIZE 1000
#define MAX_LINE 1024

typedef struct bench_args {
    const char *host;
    int port;
    int client_id;
    int ops_per_client;
    int read_pct;
    unsigned int seed;
    long successful_ops;
    long failed_ops;
} bench_args_t;

static void usage(const char *prog) {
    fprintf(stderr,
        "usage: %s <host> <port> <num_clients> <ops_per_client> <read_pct>\n",
        prog);
}

static double elapsed_seconds(struct timespec start, struct timespec end) {
    double sec = (double)(end.tv_sec - start.tv_sec);
    double nsec = (double)(end.tv_nsec - start.tv_nsec) / 1000000000.0;
    return sec + nsec;
}

static int connect_to_server(const char *host, int port) {
    struct addrinfo hints;
    struct addrinfo *result = NULL;
    struct addrinfo *rp = NULL;
    char port_str[32];
    int fd = -1;

    snprintf(port_str, sizeof(port_str), "%d", port);

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    int rc = getaddrinfo(host, port_str, &hints, &result);
    if (rc != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rc));
        return -1;
    }

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) {
            continue;
        }

        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            break;
        }

        close(fd);
        fd = -1;
    }

    freeaddrinfo(result);
    return fd;
}

static int send_all(int fd, const char *buf) {
    size_t len = strlen(buf);
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, 0);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        if (n == 0) {
            return -1;
        }

        sent += (size_t)n;
    }

    return 0;
}

static int read_line(int fd, char *buf, size_t cap) {
    size_t used = 0;

    if (cap == 0) {
        return -1;
    }

    while (used + 1 < cap) {
        char c;
        ssize_t n = recv(fd, &c, 1, 0);

        if (n == 0) {
            if (used == 0) {
                return 0;
            }
            break;
        }

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }

        if (c == '\n') {
            break;
        }

        if (c == '\r') {
            continue;
        }

        buf[used++] = c;
    }

    buf[used] = '\0';
    return 1;
}

static void *client_thread_main(void *arg) {
    bench_args_t *args = (bench_args_t *)arg;
    int fd = connect_to_server(args->host, args->port);

    if (fd < 0) {
        fprintf(stderr, "client %d: failed to connect\n", args->client_id);
        args->failed_ops = args->ops_per_client;
        return NULL;
    }

    char cmd[MAX_LINE];
    char reply[MAX_LINE];

    /*
     * Warm up part of the shared key pool so GET operations are more likely to hit.
     * These warmup PUTs are not counted as requested benchmark operations,
     * but they do occur during the measured client thread lifetime.
     */
    for (int i = 0; i < 20; i++) {
        int key_id = rand_r(&args->seed) % KEY_POOL_SIZE;
        snprintf(cmd, sizeof(cmd), "PUT k%d warm%d\n", key_id, args->client_id);

        if (send_all(fd, cmd) != 0 || read_line(fd, reply, sizeof(reply)) <= 0) {
            args->failed_ops++;
        }
    }

    for (int i = 0; i < args->ops_per_client; i++) {
        int key_id = rand_r(&args->seed) % KEY_POOL_SIZE;
        int roll = rand_r(&args->seed) % 100;

        if (roll < args->read_pct) {
            snprintf(cmd, sizeof(cmd), "GET k%d\n", key_id);
        } else {
            snprintf(cmd, sizeof(cmd), "PUT k%d v%d_%d\n",
                     key_id, args->client_id, i);
        }

        if (send_all(fd, cmd) != 0) {
            args->failed_ops++;
            continue;
        }

        if (read_line(fd, reply, sizeof(reply)) <= 0) {
            args->failed_ops++;
            continue;
        }

        /*
         * Any valid server response counts as a completed benchmark operation.
         * GET may return either VALUE or NOT_FOUND depending on timing and key
         * distribution, but both mean the server completed the operation.
         */
        args->successful_ops++;
    }

    send_all(fd, "QUIT\n");
    read_line(fd, reply, sizeof(reply));
    close(fd);

    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 6) {
        usage(argv[0]);
        return 1;
    }

    const char *host      = argv[1];
    int port              = atoi(argv[2]);
    int num_clients       = atoi(argv[3]);
    int ops_per_client    = atoi(argv[4]);
    int read_pct          = atoi(argv[5]);

    if (port <= 0 || num_clients < 1 || ops_per_client < 1 ||
        read_pct < 0 || read_pct > 100) {
        usage(argv[0]);
        return 1;
    }

    pthread_t *threads = calloc((size_t)num_clients, sizeof(pthread_t));
    bench_args_t *args = calloc((size_t)num_clients, sizeof(bench_args_t));
    int *created = calloc((size_t)num_clients, sizeof(int));

    if (threads == NULL || args == NULL || created == NULL) {
        perror("calloc");
        free(threads);
        free(args);
        free(created);
        return 1;
    }

    struct timespec start;
    struct timespec end;

    if (clock_gettime(CLOCK_MONOTONIC, &start) != 0) {
        perror("clock_gettime");
        free(threads);
        free(args);
        free(created);
        return 1;
    }

    for (int i = 0; i < num_clients; i++) {
        args[i].host = host;
        args[i].port = port;
        args[i].client_id = i;
        args[i].ops_per_client = ops_per_client;
        args[i].read_pct = read_pct;
        args[i].seed = (unsigned int)time(NULL) ^ (unsigned int)(i * 2654435761U);
        args[i].successful_ops = 0;
        args[i].failed_ops = 0;

        if (pthread_create(&threads[i], NULL, client_thread_main, &args[i]) != 0) {
            perror("pthread_create");
            args[i].failed_ops = ops_per_client;
            created[i] = 0;
        } else {
            created[i] = 1;
        }
    }

    long successful_ops = 0;
    long failed_ops = 0;

    for (int i = 0; i < num_clients; i++) {
        if (created[i]) {
            pthread_join(threads[i], NULL);
        }
        successful_ops += args[i].successful_ops;
        failed_ops += args[i].failed_ops;
    }

    if (clock_gettime(CLOCK_MONOTONIC, &end) != 0) {
        perror("clock_gettime");
        free(threads);
        free(args);
        free(created);
        return 1;
    }

    double elapsed = elapsed_seconds(start, end);
    long requested_ops = (long)num_clients * (long)ops_per_client;
    double throughput = (elapsed > 0.0) ? ((double)successful_ops / elapsed) : 0.0;

    printf("clients=%d ops_per_client=%d read_pct=%d total_requested_ops=%ld successful_ops=%ld failed_ops=%ld elapsed_s=%.6f throughput_ops_per_s=%.2f\n",
           num_clients,
           ops_per_client,
           read_pct,
           requested_ops,
           successful_ops,
           failed_ops,
           elapsed,
           throughput);

    free(threads);
    free(args);
    free(created);

    return (failed_ops == 0) ? 0 : 1;
}
