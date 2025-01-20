/* utils.h: SMQ Utility macros */

#ifndef SMQ_UTILS_H
#define SMQ_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Logging */

#ifndef NDEBUG
#define debug(M, ...) \
    fprintf(stderr, "[%09lu] DEBUG %s:%d:%s: " M "\n", pthread_self(), __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#else
#define debug(M, ...)
#endif

#define info(M, ...) \
    fprintf(stderr, "[%09lu] INFO  " M "\n", pthread_self(), ##__VA_ARGS__)

#define error(M, ...) \
    fprintf(stderr, "[%09lu] ERROR " M "\n", pthread_self(), ##__VA_ARGS__)

/* Miscellaneous */

#define chomp(s)            if (strlen(s)) { s[strlen(s) - 1] = 0; }
#define min(a, b)           ((a) < (b) ? (a) : (b))
#define streq(a, b)         (strcmp(a, b) == 0)

#define compute_stoptime(ts, timeout) \
    do { \
        clock_gettime(CLOCK_REALTIME, &ts); \
        ts.tv_sec  += (timeout / 1000); \
        ts.tv_nsec += (timeout % 1000) * 1000000; \
    } while(0);

#endif

/* vim: set expandtab sts=4 sw=4 ts=8 ft=c: */
