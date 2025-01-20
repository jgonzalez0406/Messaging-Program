/* queue.c: Concurrent Queue of Requests */

#include "smq/queue.h"
#include "smq/utils.h"
#include <time.h>

/**
 * Create queue structure.
 * @return  Newly allocated queue structure.
 **/
Queue * queue_create() {
    Queue *q = calloc(1, sizeof(Queue));
    // Continue only if the queue was created successfully
    if (q) {
        q->running = true;
        q->size = 0;
        q->head = NULL;
        q->tail = NULL;
        mutex_init(&q->lock, NULL);
        cond_init(&q->consumed, NULL);
        cond_init(&q->produced, NULL);

        return q;
    }

    return NULL;
}

/**
 * Delete queue structure.
 * @param   q       Queue structure.
 **/
void queue_delete(Queue *q) {
    if (q) {
        mutex_lock(&q->lock);
        q->running = false;

        // Free remaining requests in the queue
        while (q->head) {
            Request *r = q->head;
            q->head = r->next;

            request_delete(r);
        }

        // Don't destroy mutex or condition variables
        // They will just "disappear" when the program ends
        mutex_unlock(&(q->lock));
        free(q);
    }
}

/**
 * Shutdown queue.
 * @param   q       Queue structure.
 **/
void queue_shutdown(Queue *q) {
    mutex_lock(&q->lock);
    if (q) {
        q->running = false;

        mutex_unlock(&q->lock);
        return;
    }
    mutex_unlock(&q->lock);
    return;

}

/**
 * Push message to the back of queue.
 * @param   q       Queue structure.
 * @param   r       Request structure.
 **/
void queue_push(Queue *q, Request *r) {
    mutex_lock(&q->lock);

    if (!q->running) {
        mutex_unlock(&q->lock);
        return;
    }

    r->next = NULL;

    // Add the request to the queue
    if (q->size == 0) {
        q->head = r;
        q->tail = r;
    } else {
        q->tail->next = r;
        q->tail = r;
    }
    
    q->size++;

    // Signal waiting consumers that there is a new request
    cond_signal(&q->produced);

    mutex_unlock(&q->lock);

    return;
    
}

/**
 * Pop message from the front of queue (block until there is something to return).
 * @param   q       Queue structure.
 * @param   timeout How long to wait before re-checking condition (ms).
 * @return  Request structure.
 **/
Request * queue_pop(Queue *q, time_t timeout) {
    mutex_lock(&q->lock);
    if (!q) {
        return NULL;
    }

    struct timespec ts;
    compute_stoptime(ts, timeout);

    // Wait for an item to be produced or for a shutdown, with a timeout
    while (q->size == 0) {
        int ret = pthread_cond_timedwait(&q->produced, &q->lock, &ts);

        if (ret == ETIMEDOUT) {
            mutex_unlock(&q->lock);
            return NULL;  // Timeout occurred, return NULL
        }
    }

    // Get the request from the front of the queue
    Request *r = q->head;

    if (q->size == 1) {
        q->head = NULL;
        q->tail = NULL;
    } else {
        q->head = q->head->next;
    }

    q->size--;
    r->next = NULL;

    // Signal that an item has been consumed
    cond_signal(&q->consumed);

    pthread_mutex_unlock(&q->lock);

    return r;  // Return the popped request
}

/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */
