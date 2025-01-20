/* request.h: SMQ Request structure */

#ifndef SMQ_REQUEST_H
#define SMQ_REQUEST_H

/* Structures */

typedef struct Request Request;
struct Request {
    char    *method;    // Method string performed by Request
    char    *url;       // URL string to send with Request
    char    *body;      // Body string to send in Request

    Request *next;      // Pointer to next Request in sequence
};

/* Functions */

Request *   request_create(const char *method, const char *url, const char *body);
void        request_delete(Request *r);

char *      request_perform(Request *r, long timeout);

#endif

/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */ 
