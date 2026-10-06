/*
 * kvserver.c -- Mini-KV server entry point
 *
 * Project 2, CprE 3080, Spring 2026
 *
 * Starter scaffolding: this file gives you a working TCP listener and an
 * argument parser. Everything else -- accept loop, protocol, hash table,
 * thread pool, RW locking, TTL sweeper -- is yours to write.
 *
 * Build: run `make` in this directory. See the provided Makefile.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include "kv.h"

/* -------- Globals ------------------------------------------------------- */

volatile sig_atomic_t g_shutdown = 0;
static work_queue_t g_workq;

static void sigint_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

/* Worker threads pop accepted connection fds and serve one client at a time. */
static void *worker_main(void *arg) {
    work_queue_t *q = arg;
    while (1) {
        int conn;
        if (workq_pop(q, &conn) != 0) {
            break;
        }
        handle_client(conn);
        close(conn);
    }
    return NULL;
}
/* -------- Socket helpers ------------------------------------------------ */

/* Create a listening TCP socket bound to the given port. Returns fd or -1. */
static int make_listen_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        close(fd);
        return -1;
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, 64) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }
    return fd;
}

/* -------- Entry point --------------------------------------------------- */

static void usage(const char *prog) {
    fprintf(stderr,
        "usage: %s <port> <num_workers> <num_buckets> [sweeper_interval_ms]\n"
        "   port                TCP port to listen on (1-65535)\n"
        "   num_workers         number of worker threads (>=1)\n"
        "   num_buckets         hash-table bucket count (>=1)\n"
        "   sweeper_interval_ms default 500\n",
        prog);
}

int main(int argc, char **argv) {
    if (argc < 4 || argc > 5) {
        usage(argv[0]);
        return 1;
    }
    int port         = atoi(argv[1]);
    int num_workers  = atoi(argv[2]);
    int num_buckets  = atoi(argv[3]);
    int sweeper_ms   = (argc == 5) ? atoi(argv[4]) : 500;

    if (port <= 0 || port > 65535 || num_workers < 1 ||
        num_buckets < 1 || sweeper_ms <= 0) {
        usage(argv[0]);
        return 1;
    }

    /* Install Ctrl-C handlers for clean shutdown. */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigint_handler;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Ignore SIGPIPE: writes to closed sockets should fail with EPIPE, not
     * kill the server. */
    signal(SIGPIPE, SIG_IGN);

    int listen_fd = make_listen_socket(port);
    if (listen_fd < 0) {
        return 1;
    }

    fprintf(stderr,
        "kvserver: listening on port %d "
        "(workers=%d, buckets=%d, sweeper=%dms)\n",
        port, num_workers, num_buckets, sweeper_ms);

    if (kv_table_init((size_t)num_buckets) != 0) {
        fprintf(stderr, "kv_table_init failed\n");
        close(listen_fd);
        return 1;
    }

    pthread_t *workers = calloc((size_t)num_workers, sizeof(pthread_t));
    if (workers == NULL) {
        perror("calloc workers");
        kv_table_destroy();
        close(listen_fd);
        return 1;
    }

    int queue_capacity = 2 * num_workers;

    if (workq_init(&g_workq, queue_capacity) != 0) {
        fprintf(stderr, "workq_init failed\n");
        free(workers);
        kv_table_destroy();
        close(listen_fd);
        return 1;
    }
    
    int workers_started = 0;

    for (int i = 0; i < num_workers; i++) {
        if (pthread_create(&workers[i], NULL, worker_main, &g_workq) != 0) {
            perror("pthread_create");
            g_shutdown = 1;
            break;
        }

        workers_started++;
    }

    pthread_t sweeper;
    sweeper_args_t sweeper_args;
    int sweeper_started = 0;

    sweeper_args.interval_ms = sweeper_ms;

    if (!g_shutdown) {
        if (pthread_create(&sweeper, NULL, sweeper_main, &sweeper_args) != 0) {
            perror("pthread_create sweeper");
            g_shutdown = 1;
        } else {
            sweeper_started = 1;
        }
    }

    if (g_shutdown) {
        workq_shutdown(&g_workq);
        for (int i = 0; i < workers_started; i++) {
            pthread_join(workers[i], NULL);
        }
        if (sweeper_started) {
            pthread_join(sweeper, NULL);
        }
        workq_destroy(&g_workq);
        free(workers);
        kv_table_destroy();
        close(listen_fd);
        return 1;
    }

    while (!g_shutdown) {
        int conn = accept(listen_fd, NULL, NULL);
        if (conn < 0) {
            if (errno == EINTR) {
                if (g_shutdown) {
                    break;
                }
                continue;
            }
            perror("accept");
            g_shutdown = 1;
            break;
        }

        if (workq_push(&g_workq, conn) != 0) {
            close(conn);
            g_shutdown = 1;
            break;
        }
    }

    g_shutdown = 1;
    workq_shutdown(&g_workq);

    for (int i = 0; i < workers_started; i++) {
        pthread_join(workers[i], NULL);
    }

    if (sweeper_started) {
        pthread_join(sweeper, NULL);
    }
    workq_destroy(&g_workq);
    free(workers);

    kv_table_destroy();
    close(listen_fd);
    return 0;
}
