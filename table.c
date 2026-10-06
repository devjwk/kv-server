#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "kv.h"

kv_table_t g_table;
kv_stats_t g_stats;

static unsigned long hash_key(const char *s) {
    unsigned long hash = 5381;
    int c;

    while ((c = (unsigned char)*s++) != '\0') {
        hash = ((hash << 5) + hash) + (unsigned long)c;
    }

    return hash;
}

static int valid_key_or_value(const char *s, size_t max_len) {
    size_t len;

    if (s == NULL) {
        return 0;
    }

    len = strlen(s);
    if (len == 0 || len > max_len) {
        return 0;
    }

    return 1;
}

static void stats_inc(unsigned long *counter) {
    if (pthread_mutex_lock(&g_stats.lock) != 0) {
        perror("pthread_mutex_lock");
        return;
    }

    (*counter)++;

    if (pthread_mutex_unlock(&g_stats.lock) != 0) {
        perror("pthread_mutex_unlock");
    }
}

int kv_table_init(size_t num_buckets) {
    if (num_buckets == 0) {
        return -1;
    }

    memset(&g_table, 0, sizeof(g_table));
    memset(&g_stats, 0, sizeof(g_stats));

    if (pthread_mutex_init(&g_stats.lock, NULL) != 0) {
        perror("pthread_mutex_init");
        return -1;
    }

    g_table.buckets = calloc(num_buckets, sizeof(kv_entry_t *));
    if (g_table.buckets == NULL) {
        perror("calloc");
        pthread_mutex_destroy(&g_stats.lock);
        return -1;
    }

    g_table.num_buckets = num_buckets;
    g_table.num_keys = 0;

    if (pthread_rwlock_init(&g_table.lock, NULL) != 0) {
        perror("pthread_rwlock_init");
        free(g_table.buckets);
        g_table.buckets = NULL;
        pthread_mutex_destroy(&g_stats.lock);
        return -1;
    }

    g_stats.start_time = time(NULL);

    return 0;
}

void kv_table_destroy(void) {
    if (g_table.buckets == NULL) {
        return;
    }

    for (size_t i = 0; i < g_table.num_buckets; i++) {
        kv_entry_t *cur = g_table.buckets[i];

        while (cur != NULL) {
            kv_entry_t *next = cur->next;
            free(cur);
            cur = next;
        }
    }

    free(g_table.buckets);
    g_table.buckets = NULL;
    g_table.num_buckets = 0;
    g_table.num_keys = 0;

    pthread_rwlock_destroy(&g_table.lock);
    pthread_mutex_destroy(&g_stats.lock);
}

int kv_get(const char *key, char *out_val, size_t out_cap) {
    if (!valid_key_or_value(key, MAX_KEY_LEN) ||
        out_val == NULL ||
        out_cap == 0 ||
        g_table.buckets == NULL) {
        return -1;
    }

    if (pthread_rwlock_rdlock(&g_table.lock) != 0) {
        perror("pthread_rwlock_rdlock");
        return -1;
    }

    size_t idx = hash_key(key) % g_table.num_buckets;
    kv_entry_t *cur = g_table.buckets[idx];

    while (cur != NULL) {
        if (strcmp(cur->key, key) == 0) {
            time_t now = time(NULL);
            if (cur->has_ttl && cur->expired_at <= now) {
                pthread_rwlock_unlock(&g_table.lock);
                stats_inc(&g_stats.misses);
                return -1;
            }
            snprintf(out_val, out_cap, "%s", cur->value);
            pthread_rwlock_unlock(&g_table.lock);
            stats_inc(&g_stats.hits);
            return 0;
        }

        cur = cur->next;
    }

    pthread_rwlock_unlock(&g_table.lock);
    stats_inc(&g_stats.misses);
    return -1;
}

int kv_put(const char *key, const char *val, int ttl_seconds) {
    if (!valid_key_or_value(key, MAX_KEY_LEN) ||
        !valid_key_or_value(val, MAX_VAL_LEN) ||
        g_table.buckets == NULL) {
        return -1;
    }

    if (pthread_rwlock_wrlock(&g_table.lock) != 0) {
        perror("pthread_rwlock_wrlock");
        return -1;
    }
    time_t now = time(NULL);
    size_t idx = hash_key(key) % g_table.num_buckets;
    kv_entry_t *cur = g_table.buckets[idx];

    while (cur != NULL) {
        if (strcmp(cur->key, key) == 0) {
            snprintf(cur->value, sizeof(cur->value), "%s", val);
            if (ttl_seconds > 0) {
                cur->has_ttl = 1;
                cur->expired_at = now + ttl_seconds;
            } else {
                cur->has_ttl = 0;
                cur->expired_at = 0;
            }
            pthread_rwlock_unlock(&g_table.lock);
            stats_inc(&g_stats.puts);
            return 0;
        }

        cur = cur->next;
    }

    kv_entry_t *entry = malloc(sizeof(kv_entry_t));
    if (entry == NULL) {
        perror("malloc");
        pthread_rwlock_unlock(&g_table.lock);
        return -1;
    }

    snprintf(entry->key, sizeof(entry->key), "%s", key);
    snprintf(entry->value, sizeof(entry->value), "%s", val);
    if (ttl_seconds > 0) {
        entry->has_ttl = 1;
        entry->expired_at = now + ttl_seconds;
    } else {
        entry->has_ttl = 0;
        entry->expired_at = 0;
    }
    entry->next = g_table.buckets[idx];

    g_table.buckets[idx] = entry;
    g_table.num_keys++;
    pthread_rwlock_unlock(&g_table.lock);
    stats_inc(&g_stats.puts);

    return 0;
}

int kv_del(const char *key) {
    if (!valid_key_or_value(key, MAX_KEY_LEN) ||
        g_table.buckets == NULL) {
        return -1;
    }

    if (pthread_rwlock_wrlock(&g_table.lock) != 0) {
        perror("pthread_rwlock_wrlock");
        return -1;
    }

    size_t idx = hash_key(key) % g_table.num_buckets;
    kv_entry_t *cur = g_table.buckets[idx];
    kv_entry_t *prev = NULL;

    while (cur != NULL) {
        if (strcmp(cur->key, key) == 0) {
            if (prev == NULL) {
                g_table.buckets[idx] = cur->next;
            } else {
                prev->next = cur->next;
            }

            free(cur);
            g_table.num_keys--;

            pthread_rwlock_unlock(&g_table.lock);
            stats_inc(&g_stats.dels);
            return 0;
        }

        prev = cur;
        cur = cur->next;
    }

    pthread_rwlock_unlock(&g_table.lock);
    stats_inc(&g_stats.misses);
    return -1;
}

int kv_remove_expired(void) {
    if (g_table.buckets == NULL) {
        return 0;
    }

    time_t now = time(NULL);
    int removed = 0;

    if (pthread_rwlock_wrlock(&g_table.lock) != 0) {
        perror("pthread_rwlock_wrlock");
        return 0;
    }

    for (size_t i = 0; i < g_table.num_buckets; i++) {
        kv_entry_t *cur = g_table.buckets[i];
        kv_entry_t *prev = NULL;

        while (cur != NULL) {
            kv_entry_t *next = cur->next;
            if (cur->has_ttl && cur->expired_at <= now) {
                if (prev == NULL) {
                    g_table.buckets[i] = next;
                } else {
                    prev->next = next;
                }
                free(cur);
                g_table.num_keys--;
                removed++;
            } else {
                prev = cur;
            }
            cur = next;
        }
    }
    pthread_rwlock_unlock(&g_table.lock);
    return removed;
}

void *sweeper_main(void *arg) {
    sweeper_args_t *args = (sweeper_args_t *)arg;
    int interval_ms = args->interval_ms;

    while (!g_shutdown) {
        struct timespec ts;
        ts.tv_sec = interval_ms / 1000;
        ts.tv_nsec = (long)(interval_ms % 1000) * 1000000L;

        while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
            continue;
        }

        if (g_shutdown) {
            break;
        }

        kv_remove_expired();
    }

    return NULL;
}
