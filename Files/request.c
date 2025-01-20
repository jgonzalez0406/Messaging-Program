/* Request.c: Request structure */

#include "smq/request.h"
#include "smq/utils.h"

#include <stdlib.h>
#include <string.h>

#include <curl/curl.h>

/* Internal Structures */

typedef struct {
    char *       data;      // Response data string
    size_t       size;      // Response data length
} Response;

typedef struct {
    const char * data;      // Payload data string
    size_t       offset;    // Payload data offset
} Payload;

/* Internal Functions */

/**
 * Writer function: Copy data up to size*nmemb from ptr to userdata (Response).
 *
 * @param   ptr         Pointer to delivered data.
 * @param   size        Always 1.
 * @param   nmemb       Size of the delivered data.
 * @param   userdata    Pointer to user-provided Response structure.
 **/
size_t  request_writer(char *ptr, size_t size, size_t nmemb, void *userdata) {
    // Cast userdata to Response
    size_t realsize = size * nmemb;
    Response *r = (Response *)userdata;
    
    // Reallocate memory for response data
    char *ptr2 = realloc(r->data, r->size + realsize + 1);
    if(!ptr2) {
        free(r->data);
        return 0;  /* out of memory! */
    }
    r->data = ptr2;
    memcpy(&r->data[r->size], ptr, realsize);
    r->size += realsize;
    r->data[r->size] = '\0';
    
    return realsize;
}

/**
 * Reader function: Copy data up to size*nmemb from userdata (Payload) to ptr.
 *
 * @param   ptr         Pointer to data to deliver.
 * @param   size        Always 1.
 * @param   nmemb       Size of the data buffer.
 * @param   userdata    Pointer to user-provided Payload structure.
 **/
size_t  request_reader(char *ptr, size_t size, size_t nmemb, void *userdata) {
    // Cast userdata to Payload
    Payload *payload = (Payload *)userdata;

    // Check for NULL
    if (payload->data == NULL) {
        return 0;
    }

    size_t realsize = size * nmemb;
    size_t res;
    
    if (strlen(payload->data) - payload->offset > realsize) {
        res = realsize;
    } else {
        res = strlen(payload->data) - payload->offset;
    }

    // Copy data from payload to ptr
    memcpy(ptr, payload->data + payload->offset, res);
    payload->offset += res;

    return res;
}

/* Functions */

/**
 * Create Request structure.
 * @param   method      Request method string.
 * @param   uri         Request uri string.
 * @param   body        Request body string.
 * @return  Newly allocated Request structure.
 **/
Request * request_create(const char *method, const char *url, const char *body) {
    Request *r = (Request *)calloc(1, sizeof(Request));

    // Check for NULL
    if (!r) {
        return NULL;
    }

    // Copy method, url, and body if they exist
    if (method) {
        r->method = strdup(method);
    }
    
    if (url) {
        r->url = strdup(url);
    }

    if (body) {
        r->body = strdup(body);
    }

    return r;
        
}

/**
 * Delete Request structure.
 * @param   r           Request structure.
 **/
void request_delete(Request *r) {
    // Check for NULL
    if (r == NULL) {
        return;
    }

    if (r->method != NULL) {
        free(r->method);
    }

    if (r->url != NULL) {
        free(r->url);
    }

    if (r->body != NULL) {
        free(r->body);
    }

    free(r);
}

/**
 * Perform HTTP request using libcurl.
 *
 *  1. Initialize curl.
 *  2. Set curl options.
 *  3. Perform curl.
 *  4. Cleanup curl.
 *
 * Note: this must support GET, PUT, and DELETE methods, and adjust options as
 * necessary to support these methods.
 *
 * @param   r           Request structure.
 * @param   timeout     Maximum total HTTP transaction time (in milliseconds).
 * @return  Body of HTTP response (NULL if error or timeout).
 **/
char * request_perform(Request *r, long timeout) {
    // Initialize CURL client
    CURL *curl = curl_easy_init();  // Initialize a curl session
    
    if (!curl) {
        return NULL;
    }

    // Initialize response structure
    Response response = {0};
    Payload  payload  = {.data = r->body, .offset = 0};

    char * url = r->url;

    // Set CURL options
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, request_writer);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1);

    if (streq(r->method, "PUT")) {
        // Perform PUT request
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1);
        if (r->body == NULL)
        {
            curl_easy_setopt(curl, CURLOPT_INFILESIZE, 0);
        }
        else
        {
            curl_easy_setopt(curl, CURLOPT_READFUNCTION, request_reader);
            curl_easy_setopt(curl, CURLOPT_READDATA, &payload);
            curl_easy_setopt(curl, CURLOPT_INFILESIZE, (curl_off_t)strlen(r->body));
        }

    } else if (streq(r->method, "DELETE")) {
        // Perform DELETE request
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    }

    // Perform CURL
    if (curl_easy_perform(curl) != CURLE_OK) {
        // Cleanup CURL
        curl_easy_cleanup(curl);
        free(response.data);
        return NULL;
    }

    // Cleanup CURL
    curl_easy_cleanup(curl);

    // Return response data
    if (response.data == NULL) {
        return NULL;
    }

    return response.data;
}

/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */
