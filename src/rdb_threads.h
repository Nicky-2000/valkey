#ifndef __RDB_THREADS_H__
#define __RDB_THREADS_H__

#include "server.h"
#include "thread_common.h"

#define BUFFER_FREE 0
#define BUFFER_READY 1


#define WORKER_BUFFER_SIZE  16*(1024*1024) // 16MB Buffer

typedef struct {
    rio rio;                        // rio structs for in-memory buffering
    atomic_int buffer_status;       // Indicates if the buffer can be read/written 
    pthread_mutex_t buffer_mutex;   // Protects the status and rio buffer
    pthread_cond_t buffer_cond;     // For signaling between worker and main thread about buffer status
} RdbSaveWorkerBuffer;

// NOTE: The next iteration will use a mod operation to assign threads buckets. So this will change.
// The changed version will be something along the lines of 
/*
typedef struct {
    int start_index;
    int jump_size; Jump size would just be the num_worker_threads
} BucketRange;
*/
typedef struct {
    int start_index;
    int end_index;
} BucketRange;

typedef struct {
    int dbid;
    hashtable *ht;                  // The hashtable this with buckets for this thread to process
    BucketRange bucket_range;       // The logical range of buckets this thread is responsible for processing
    RdbSaveWorkerBuffer *worker_buffer;    // Buffer to output the key/values in encoded RDB format
    atomic_long keys_processed;     
    atomic_bool is_done;
} RdbSaveThreadArgs;


void initRDBThreads(int per_thread_queue_size);
void killRDBThreads(void);

ssize_t rdbSaveDbMultiThreaded(rio *rdb, int dbid, long *key_counter, char *pname);


#endif // __RDB_THREADS_H__