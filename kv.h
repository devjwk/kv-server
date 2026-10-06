/*
 * kv.h -- Mini-KV server: shared declarations
 *
 * Project 2, CprE 3080, Spring 2026
 *
 * You may modify this file. It is provided as a starting point, not a rigid
 * interface. If your design benefits from additional fields or types, add them.
 */
#ifndef KV_H
#define KV_H

#include <stddef.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>
#include <signal.h>
/* -------- Protocol constants (do NOT change) ---------------------------- */

#define MAX_KEY_LEN    256
#define MAX_VAL_LEN    256
#define MAX_LINE_LEN   (MAX_KEY_LEN + MAX_VAL_LEN + 64)  /* + command + ttl */

/* Response strings. Each response is one line ending in '\n'. */
#define RESP_OK        "OK\n"
#define RESP_BYE       "BYE\n"
#define RESP_NOTFOUND  "NOT_FOUND\n"

/* -------- Mini-KV data structures --------------------------------------- */

typedef struct kv_entry {
    char key[MAX_KEY_LEN + 1];
    char value[MAX_VAL_LEN + 1];
    /* TTL metadata. has_ttl == 0 means the entry never expires. */
    int has_ttl;
    time_t expired_at;

    /* Next entry in the bucket chain. */
    struct kv_entry *next;
} kv_entry_t;   

typedef struct kv_table {
    kv_entry_t **buckets;
    size_t num_buckets;
    size_t num_keys;

    /* Single table-wide reader-writer lock. */
    pthread_rwlock_t lock;
} kv_table_t;

typedef struct kv_stats {
    unsigned long hits;
    unsigned long misses;
    unsigned long puts;
    unsigned long dels;
    unsigned long active_conns;
    time_t start_time;

    pthread_mutex_t lock;
} kv_stats_t;

/* Bounded FIFO queue of accepted connection file descriptors. */
typedef struct work_queue {
    int *fds;
    int capacity;
    int head;
    int tail;
    int count;
    int shutdown;

    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} work_queue_t;

typedef struct sweeper_args {
    int interval_ms;
} sweeper_args_t;

/* Work queue operations (Stage 2) */
int workq_init(work_queue_t *q, int capacity);
void workq_destroy(work_queue_t *q);

int workq_push(work_queue_t *q, int fd);
int workq_pop(work_queue_t *q, int *out_fd);

void workq_shutdown(work_queue_t *q);

/* -------- Global server state ------------------------------------------ */
extern kv_table_t g_table;
extern kv_stats_t g_stats;
extern volatile sig_atomic_t g_shutdown;

/* -------- Function prototypes you will likely want ---------------------- */

/* Protocol / connection handling (Stage 1) */
void handle_client(int conn_fd);        /* loop: read line, parse, reply */

/* Hash-table operations. Return 0 on success, -1 on not-found / error. */
int kv_table_init(size_t num_buckets);
void kv_table_destroy(void);

int  kv_get(const char *key, char *out_val, size_t out_cap);
int  kv_put(const char *key, const char *val, int ttl_seconds);
int  kv_del(const char *key); 

int kv_remove_expired(void);
void *sweeper_main(void *arg);

#endif /* KV_H */
