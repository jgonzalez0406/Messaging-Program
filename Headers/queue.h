/* queue.h: SMQ Concurrent Queue of Requests */

#ifndef SMQ_QUEUE_H
#define SMQ_QUEUE_H

#include "smq/request.h"
#include "smq/thread.h"

#include <stdbool.h>

/* Structures */

typedef struct Queue Queue;
struct Queue {
    Request *head;      // First request in the queue.
    Request *tail;      // Last request in the queue.
    size_t   size;      // Total number of requests in the queue.
    bool     running;   // Whether or not the queue is running (active).

    // TODO: Add any necessary thread and synchromization primitives.
    Mutex  lock;        // Queue 1
    Cond   cond;        // Queue 2

    Cond   consumed;    // Queue 3
    Cond   produced;    // Queue 3

};

/* Functions */

Queue *     queue_create();
void        queue_delete(Queue *q);

void        queue_shutdown(Queue *q);

void        queue_push(Queue *q, Request *r);
Request *   queue_pop(Queue *q, time_t timeout);

#endif

/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */
