#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kv.h"

int workq_init(work_queue_t *q, int capacity) {
    if (q == NULL || capacity <= 0) {
        return -1;
    }

    memset(q, 0, sizeof(*q));

    q->fds = malloc((size_t)capacity * sizeof(int));
    if (q->fds == NULL) {
        perror("malloc");
        return -1;
    }

    q->capacity = capacity;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->shutdown = 0;

    if (pthread_mutex_init(&q->mutex, NULL) != 0) {
        free(q->fds);
        q->fds = NULL;
        return -1;
    }

    if (pthread_cond_init(&q->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&q->mutex);
        free(q->fds);
        q->fds = NULL;
        return -1;
    }

    if (pthread_cond_init(&q->not_full, NULL) != 0) {
        pthread_cond_destroy(&q->not_empty);
        pthread_mutex_destroy(&q->mutex);
        free(q->fds);
        q->fds = NULL;
        return -1;
    }

    return 0;
}

void workq_destroy(work_queue_t *q) {
    if (q == NULL) {
        return;
    }

    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->mutex);

    free(q->fds);
    q->fds = NULL;
    q->capacity = 0;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->shutdown = 0;
}

int workq_push(work_queue_t *q, int fd) {
    if (q == NULL) {
        return -1;
    }

    if (pthread_mutex_lock(&q->mutex) != 0) {
        perror("pthread_mutex_lock");
        return -1;
    }

    while (q->count == q->capacity && !q->shutdown) {
        if (pthread_cond_wait(&q->not_full, &q->mutex) != 0) {
            perror("pthread_cond_wait");
            pthread_mutex_unlock(&q->mutex);
            return -1;
        }
    }

    if (q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }

    q->fds[q->tail] = fd;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);

    return 0;
}

int workq_pop(work_queue_t *q, int *out_fd) {
    if (q == NULL || out_fd == NULL) {
        return -1;
    }

    if (pthread_mutex_lock(&q->mutex) != 0) {
        perror("pthread_mutex_lock");
        return -1;
    }

    while (q->count == 0 && !q->shutdown) {
        if (pthread_cond_wait(&q->not_empty, &q->mutex) != 0) {
            perror("pthread_cond_wait");
            pthread_mutex_unlock(&q->mutex);
            return -1;
        }
    }

    if (q->count == 0 && q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }

    *out_fd = q->fds[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);

    return 0;
}

void workq_shutdown(work_queue_t *q) {
    if (q == NULL) {
        return;
    }

    if (pthread_mutex_lock(&q->mutex) != 0) {
        perror("pthread_mutex_lock");
        return;
    }

    q->shutdown = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
}
