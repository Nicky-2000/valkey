#ifndef RDB_SAVE_THREAD_H
#define RDB_SAVE_THREAD_H

#include "rdb.h"
#include "sds.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>


#define BUFFER_FREE 0
#define BUFFER_READY 1


#define WORKER_BUFFER_SIZE  4*(1024*1024) // 16MB Buffer

typedef struct {
    rio rio;                        // rio structs for in-memory buffering
    atomic_int buffer_status;       // Indicates if the buffer can be read/written 
    pthread_mutex_t buffer_mutex;   // Protects the status and rio buffer
    pthread_cond_t buffer_cond;     // For signaling between worker and main thread about buffer status
} WorkerBuffer;

typedef struct {
    int start_index;
    int end_index;
} BucketRange;

typedef struct {
    int thread_id;
    int dbid;
    hashtable *ht;                  // The hashtable this with buckets for this thread to process
    BucketRange bucket_range;       // The logical range of buckets this thread is responsible for processing
    WorkerBuffer *worker_buffer;    // Buffer to output the key/values in encoded RDB format
    atomic_long keys_processed;     
    atomic_bool is_done;
} RdbSaveThreadArgs;

// Allocates and initializes RdBSaveThreadArgs that are processed by to rdbEncodeHashtableRange()
RdbSaveThreadArgs *createRdbSaveThreadArgs(int num_threads, int dbid);
void freeRdbSaveThreadArgs(int num_threads, RdbSaveThreadArgs *threadArgs);

BucketRange calculateBucketRangeForThread(hashtable * ht, int num_threads, int thread_id);
// Thread Function that serializes data in bucket range
void rdbEncodeHashtableRange(void *arg);

#endif /* RDB_SAVE_THREAD_H */