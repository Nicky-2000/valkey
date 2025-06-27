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
    //  Make this so the threads are just passed the start and end index instead of the thread id.
    int thread_id;
    int dbid;
    hashtable *ht;
    BucketRange bucket_range;
    WorkerBuffer *worker_buffer;
    atomic_long keys_processed; // Counter for keys processed.
    atomic_bool is_done; // Worker's overall completion status for the batch
} RdbSaveThreadArgs;

// Allocates and initializes RdBSaveThreadArgs that are processed by to rdbEncodeHashtableRange()
RdbSaveThreadArgs *createRdbSaveThreadArgs(int num_threads, int dbid);
void freeRdbSaveThreadArgs(int num_threads, RdbSaveThreadArgs *threadArgs);

BucketRange calculateBucketRangeForThread(hashtable * ht, int num_threads, int thread_id);
// Thread Function that serializes data in bucket range
void rdbEncodeHashtableRange(void *arg);

#endif /* RDB_SAVE_THREAD_H */
