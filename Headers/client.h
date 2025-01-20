/* client.h: Simple Message Queue client */

#ifndef SMQ_CLIENT_H
#define SMQ_CLIENT_H

#include "smq/queue.h"

#include <netdb.h>
#include <stdbool.h>
#include <time.h>

/* Structures */

typedef struct {
    char    name[1<<8];         // Name of message queue
    char    server_url[1<<8];   // URL of server

    time_t  timeout;            // Socket timeout (milliseconds)
    bool    running;            // Whether or not SMQ is running (active)

    Queue*  outgoing;           // Requests to be sent to server
    Queue*  incoming;           // Requests received from server

    // TODO: Add any necessary thread and synchromization primitives
    
    Thread  pusher;             // Pusher thread (outgoing)
    Thread  puller;             // Puller thread (incoming)
    Mutex   lock;               // Lock for shutdown
    
    Cond    cond;               // Client 2


} SMQ;

SMQ *   smq_create(const char *name, const char *host, const char *port);
void    smq_delete(SMQ *smq);

void    smq_publish(SMQ *smq, const char *topic, const char *body);
char *  smq_retrieve(SMQ *smq);

void    smq_subscribe(SMQ *smq, const char *topic);
void    smq_unsubscribe(SMQ *smq, const char *topic);

bool    smq_running(SMQ *smq);
void    smq_shutdown(SMQ *smq);

#endif

/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */
