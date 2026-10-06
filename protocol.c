#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <limits.h>
#include "kv.h"

static ssize_t read_line(int fd, char *buf, size_t cap) {
    size_t used = 0;
    if (cap == 0) {
        return -1;
    }
    while (used + 1 < cap) {
        char c;
        ssize_t n = recv(fd, &c, 1, 0);
        if (n == 0) {
            /*
             * Client closed the socket.
             */
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
    /*
     * Line was too long and did not fit in the buffer.
     */
    if (used + 1 == cap) {
        return -2;
    }
    return (ssize_t)used;
}

static int send_all(int fd, const char *msg) {
    size_t len = strlen(msg);
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = send(fd, msg + sent, len - sent, 0);

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

void handle_client(int conn_fd) {
    char line[MAX_LINE_LEN + 2];
    pthread_mutex_lock(&g_stats.lock);
    g_stats.active_conns++;
    pthread_mutex_unlock(&g_stats.lock);
    while (1) {
        ssize_t n = read_line(conn_fd, line, sizeof(line));

        if (n == 0) {
            break;
        }

        if (n == -2) {
            send_all(conn_fd, "ERROR line-too-long\n");
            break;
        }

        if (n < 0) {
            break;
        }

        if (line[0] == '\0') {
            send_all(conn_fd, "ERROR empty-command\n");
            continue;
        }

        char *tokens[5] = {0};
        int tok_count = 0;

        char *saveptr = NULL;
        char *tok = strtok_r(line, " \t", &saveptr);

        while (tok != NULL && tok_count < 5) {
            tokens[tok_count++] = tok;
            tok = strtok_r(NULL, " \t", &saveptr);
        }

        if (tok != NULL) {
            send_all(conn_fd, "ERROR too-many-args\n");
            continue;
        }

        if (tok_count == 0) {
            send_all(conn_fd, "ERROR empty-command\n");
            continue;
        }

        if (strcmp(tokens[0], "GET") == 0) {
            if (tok_count != 2) {
                send_all(conn_fd, "ERROR invalid-args\n");
                continue;
            }

            char value[MAX_VAL_LEN + 1];

            if (kv_get(tokens[1], value, sizeof(value)) == 0) {
                char response[MAX_VAL_LEN + 16];
                snprintf(response, sizeof(response), "VALUE %s\n", value);
                send_all(conn_fd, response);
            } else {
                send_all(conn_fd, RESP_NOTFOUND);
            }
        } else if (strcmp(tokens[0], "PUT") == 0) {
            int ttl_seconds = 0;

            if (tok_count == 3) {
                ttl_seconds = 0;
            } else if (tok_count == 4) {
                char *endptr = NULL;
                long ttl = strtol(tokens[3], &endptr, 10);

                if (*endptr != '\0' || ttl <= 0 || ttl > INT_MAX) {
                    send_all(conn_fd, "ERROR invalid-ttl\n");
                    continue;
                }

                ttl_seconds = (int)ttl;
            } else {
                send_all(conn_fd, "ERROR invalid-args\n");
                continue;
            }

            if (kv_put(tokens[1], tokens[2], ttl_seconds) == 0) {
                send_all(conn_fd, RESP_OK);
            } else {
                send_all(conn_fd, "ERROR invalid-put\n");
            }
        } else if (strcmp(tokens[0], "DEL") == 0) {
            if (tok_count != 2) {
                send_all(conn_fd, "ERROR invalid-args\n");
                continue;
            }

            if (kv_del(tokens[1]) == 0) {
                send_all(conn_fd, RESP_OK);
            } else {
                send_all(conn_fd, RESP_NOTFOUND);
            }
        } else if (strcmp(tokens[0], "STATS") == 0) {
            if (tok_count != 1) {
                send_all(conn_fd, "ERROR invalid-args\n");
                continue;
            }

            char response[256];
            size_t keys_snapshot;

            if (pthread_rwlock_rdlock(&g_table.lock) != 0) {
                perror("pthread_rwlock_rdlock");
                send_all(conn_fd, "ERROR stats-lock\n");
                continue;
            }
            keys_snapshot = g_table.num_keys;
            pthread_rwlock_unlock(&g_table.lock);

            unsigned long hits_snapshot;
            unsigned long misses_snapshot;
            unsigned long puts_snapshot;
            unsigned long dels_snapshot;
            unsigned long active_conns_snapshot;
            time_t start_time_snapshot;

            if (pthread_mutex_lock(&g_stats.lock) != 0) {
                perror("pthread_mutex_lock");
                send_all(conn_fd, "ERROR stats-lock\n");
                continue;
            }
            hits_snapshot = g_stats.hits;
            misses_snapshot = g_stats.misses;
            puts_snapshot = g_stats.puts;
            dels_snapshot = g_stats.dels;
            active_conns_snapshot = g_stats.active_conns;
            start_time_snapshot = g_stats.start_time;
            pthread_mutex_unlock(&g_stats.lock);

            time_t now = time(NULL);
            unsigned long uptime = 0;

            if (start_time_snapshot != (time_t)0 && now >= start_time_snapshot) {
                uptime = (unsigned long)(now - start_time_snapshot);
            }

            snprintf(response, sizeof(response),
                     "STATS keys=%zu hits=%lu misses=%lu puts=%lu dels=%lu active_conns=%lu uptime_s=%lu\n",
                     keys_snapshot,
                     hits_snapshot,
                     misses_snapshot,
                     puts_snapshot,
                     dels_snapshot,
                     active_conns_snapshot,
                     uptime);

            send_all(conn_fd, response);
        } else if (strcmp(tokens[0], "QUIT") == 0) {
            if (tok_count != 1) {
                send_all(conn_fd, "ERROR invalid-args\n");
                continue;
            }

            send_all(conn_fd, RESP_BYE);
            break;
        } else {
            send_all(conn_fd, "ERROR unknown-command\n");
        }
    }

    if (pthread_mutex_lock(&g_stats.lock) != 0) {
        perror("pthread_mutex_lock");
    } else {
        if (g_stats.active_conns > 0) {
            g_stats.active_conns--;
        }
        if (pthread_mutex_unlock(&g_stats.lock) != 0) {
            perror("pthread_mutex_unlock");
        }
    }
}
