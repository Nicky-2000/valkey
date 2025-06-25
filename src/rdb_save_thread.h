#ifndef RDB_SAVE_THREAD_H
#define RDB_SAVE_THREAD_H

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include "rdb.h"
#include "sds.h"

#define BUFFER_FREE 0
#define BUFFER_READY 1


#define WORKER_BUFFER_SIZE 16 * (1024*1024) // 16MB Buffer

typedef struct {
    rio rio; // Embedded rio structs for in-memory buffering
    sds sds_buffer; // sds string for each rio buffer
    atomic_int buffer_status; // Indicates if the buffer can be read/written 
    pthread_mutex_t buffer_mutex; // Protects access to status
    pthread_cond_t buffer_cond; // For signaling between worker and main thread about buffer status
} WorkerBuffer;

typedef struct {
    int thread_id;
    int num_threads;
    int dbid;
    hashtable *ht;
    WorkerBuffer *worker_buffer;
    atomic_bool is_done; // Worker's overall completion status for the batch
} RdbSaveThreadArgs;

typedef struct {
    int start_index;
    int end_index;
} BucketRange;

void rdbEncodeHashtableRange(void *arg);

#endif /* RDB_SAVE_THREAD_H */
