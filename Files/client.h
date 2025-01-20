/* client.c: Simple Request Queue Client */

#include "smq/client.h"

/* Internal Prototypes */

void * smq_pusher(void *);
void * smq_puller(void *);

/* External Functions */

/**
 * Create Simple Request Queue with specified name, host, and port.
 *
 * - Initialize values.
 * - Create internal queues.
 * - Create pusher and puller threads.
 *
 * @param   name        Name of client's queue.
 * @param   host        Address of server.
 * @param   port        Port of server.
 * @return  Newly allocated Simple Request Queue structure.
 **/
SMQ * smq_create(const char *name, const char *host, const char *port) {
    SMQ *smq = malloc(sizeof(SMQ));
    if(!smq) {
        fprintf(stderr, "Memory allocation failure\n");
        return NULL;
    }

    strcpy(smq->name, name);

    // Create server URL
    char url[1<<8];
    snprintf(url, sizeof(url), "%s:%s", host, port);
    strcpy(smq->server_url, url);
    // set timeout and running
    smq->timeout = 2000; // 2 seconds
    smq->running = true;

    // Create queues
    smq->outgoing = queue_create();
    if (!smq->outgoing) {
        fprintf(stderr, "Failure in outgoing queue creation");
        free(smq);
        return NULL;
    }
    smq->incoming = queue_create();
    if (!smq->incoming) {
        fprintf(stderr, "Failure in incoming queue creation");
        free(smq);
        return NULL;
    }

    // Initialize mutex
    mutex_init(&(smq->lock), NULL);

    // Create pusher and puller threads
    pthread_t pusher;
    pthread_t puller;
    thread_create(&pusher, NULL, smq_pusher, smq);
    thread_create(&puller, NULL, smq_puller, smq);

    smq->pusher = pusher;
    smq->puller = puller;

    return smq;
}

/**
 * Delete Simple Request Queue structure (and internal resources).
 * @param   smq     Simple Request Queue structure.
 **/
void smq_delete(SMQ *smq) {
    // Shutdown the SMQ
    if (!smq) {
        return;
    }
    queue_delete(smq->outgoing);
    queue_delete(smq->incoming);
    free(smq);
}

/**
 * Publish one message to topic (by placing new Request in outgoing queue).
 * @param   smq     Simple Request Queue structure.
 * @param   topic   Topic to publish to.
 * @param   body    Request body to publish.
 **/
void smq_publish(SMQ *smq, const char *topic, const char *body) {
    // If the SMQ is not running, return
    if (!smq->running) {
        return;
    }

    // Create the URL
    char url[BUFSIZ];
    sprintf(url, "%s/topic/%s", smq->server_url, topic);
    Request *r = request_create("PUT", url, body); // Create the request
    queue_push(smq->outgoing, r); // Push the request to the outgoing queue
}

/**
 * Retrieve one message (by taking a Request from incoming queue).
 *
 * Note: if the SMQ is not longer running, this will return NULL.
 *
 * @param   smq     Simple Request Queue structure.
 * @return  Newly allocated message body (must be freed).
 **/
char * smq_retrieve(SMQ *smq) {
    // If the SMQ is not running, return NULL
    if (!smq_running(smq)) {
        return NULL;
    }

    // Pop a request from the incoming queue
    Request *r = queue_pop(smq->incoming, smq->timeout);
    if (!r) {
        return NULL;
    }

    // Extract the message from the request
    char *message = r->body; 
    r->body = NULL;
    request_delete(r);
    return message;
}

/**
 * Subscribe to specified topic.
 * @param   smq     Simple Request Queue structure.
 * @param   topic   Topic string to subscribe to.
 **/
void smq_subscribe(SMQ *smq, const char *topic) {
    // body = smq_retrieve(smq); --> No Good
    char url[BUFSIZ];
    sprintf(url, "%s/subscription/%s/%s", smq->server_url, smq->name, topic);

    Request *r = request_create("PUT", url, NULL);
    // Push the request to the outgoing queue
    queue_push(smq->outgoing, r);
    return;
    
}

/**
 * Unubscribe to specified topic.
 * @param   smq     Simple Request Queue structure.
 * @param   topic   Topic string to unsubscribe from.
 **/
void smq_unsubscribe(SMQ *smq, const char *topic) {
    char url[BUFSIZ];
    sprintf(url, "%s/subscription/%s/%s", smq->server_url, smq->name, topic);

    Request *r = request_create("DELETE", url, NULL);
    // Push the request to the outgoing queue
    queue_push(smq->outgoing, r);
    return;
}

/**
 * Shutdown the Simple Request Queue by:
 *
 * 1. Shutting down the internal queues.
 * 2. Setting the internal running attribute.
 * 3. Joining internal threads.
 *
 * @param   smq      Simple Request Queue structure.
 */
void smq_shutdown(SMQ *smq) {
    // If the SMQ is not running, return
    if (!smq) {
        return;
    }
    
    // Shutdown the queues
    queue_shutdown(smq->outgoing);
    queue_shutdown(smq->incoming);

    // Set the running attribute to false
    smq->running = false;

    // Join the threads
    thread_join(smq->pusher, NULL);
    thread_join(smq->puller, NULL);

    return;
}

/**
 * Returns whether or not the Simple Request Queue is running.
 * @param   smq     Simple Request Queue structure.
 **/
bool smq_running(SMQ *smq) {
    mutex_lock(&(smq->lock));
    bool status = smq->running;
    mutex_unlock(&(smq->lock));
    return status;
}

/* Internal Functions */

/**
 * Pusher thread takes messages from outgoing queue and sends them to server.
 **/
void * smq_pusher(void *arg) {
    // Cast the argument to a SMQ
    SMQ *smq = (SMQ *)arg;
    if (!smq) {
        return NULL;
    }
    // While the SMQ is running
    while (smq_running(smq)) {
        // Pop a request from the outgoing queue
        Request *r = queue_pop(smq->outgoing, smq->timeout);
        if (!r) {
            continue;
        }
        // Perform the request
        char *response = request_perform(r, smq->timeout);
        if (!response) {
            queue_push(smq->outgoing, r);
        } else {
            request_delete(r);
            free(response);
        }
    }
    return NULL;
}

/**
 * Puller thread requests new messages from server and then puts them in
 * incoming queue.
 **/
void * smq_puller(void *arg) {
    // Cast the argument to a SMQ
    SMQ *smq = (SMQ *)arg;
    char url[BUFSIZ];
    sprintf(url, "%s/queue/%s", smq->server_url, smq->name);
    Request r = {"GET", url, NULL};
    
    // While the SMQ is running
    while (smq_running(smq)) {
        // Perform the request
        char *response = request_perform(&r, smq->timeout);
        if (!response) {
            continue;
        }
        Request *message = request_create(NULL, NULL, response);
        queue_push(smq->incoming, message);
        free(response);
    }

    return NULL;
    
}

/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */
