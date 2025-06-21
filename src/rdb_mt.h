
#ifndef RDB_MT_H
#define RDB_MT_H

#include <pthread.h>
#include <stdatomic.h>
#include "rio.h"           // Required for rio struct
#include "server.h"        // Required for serverDb, serverLog, etc.

// Struct passed to each thread
typedef struct {
    int thread_id;
    int num_threads;
    int dbid;
    pthread_mutex_t *write_mutex;
    rio *rdb;
    _Atomic long *shared_keys_processed;
    _Atomic long *shared_last_info_time_ms;
    char *pname;
} ThreadArgs;

// Run multithreaded RDB saving
ustime_t runSaveThreads(rio *rdb, int dbid, int num_threads, char *pname);

#endif // RDB_MT_H
