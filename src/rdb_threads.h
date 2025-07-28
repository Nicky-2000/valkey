#ifndef __RDB_THREADS_H__
#define __RDB_THREADS_H__

#include "server.h"
#include "thread_common.h"

/* Threshold for flushing a worker's buffer to the main RDB file (4MB). */
#define WORKER_BUFFER_DEFAULT_SIZE 4 * (1024 * 1024)
// #define WORKER_BUFFER_DEFAULT_SIZE 250
/* Maximum capacity for a worker's buffer. Keys causing this limit to be exceeded are streamed directly to RDB file (32MB). */
#define WORKER_BUFFER_CAPACITY_LIMIT 32 * (1024 * 1024)
// #define WORKER_BUFFER_CAPACITY_LIMIT 500

#define RDB_SAVE_JOB_QUEUE_SIZE 2 // Minimum size of JobQueue

typedef struct RdbSaveThreadArgs RdbSaveThreadArgs;

/* Describes a range buckets in a hashtable for a thread to process. */
typedef struct BucketStride {
    size_t start_index; // First logical bucket index for this thread
    size_t stride_size; // Step size to find the next logical bucket (typically num_worker_threads)
} BucketStride;

/* Info needed by main thread for reporting save progress*/
typedef struct MainThreadRdbInfo {
    RdbSaveThreadArgs *threadArgs;
    long *last_key_counter;
    long long *info_updated_time;
    char *pname;
} MainThreadRdbInfo;

typedef struct RdbSaveThreadArgs {
    int dbid;                   // Database ID being saved
    hashtable *ht;              // hashtable to be saved
    BucketStride bucket_stride; // Defines what buckets in a hashtable the thread is responsible for
    atomic_long keys_processed;
    ssize_t bytes_written;
    rio buf_to_file_rio;              // In-memory buffer (with max capacity) for key serialization
    rio *rdb;                         // The final target rio implementation
    pthread_mutex_t *rdb_write_mutex; // Protects access to *rdb
    int save_status;
    MainThreadRdbInfo *main_thread_report_info; // Reporting info (only set for main thread's args)
} RdbSaveThreadArgs;


/* --------- Multithreaded RDB Load:  --------- */
// New struct for chunk processing
typedef struct RdbChunkLoadThreadArgs {
    int rdbver;
    rio *chunk_rio;
    serverDb *db;
    int rdbflags;
    long long lru_clock;
    long long current_dbid;
    pthread_mutex_t *insert_mutex;
    // Add any other global state or context required by processKeyValLoad for THIS chunk
} RdbChunkLoadThreadArgs;

void initRDBThreads(int per_thread_queue_size);

void startRDBThreads(void);
void stopRDBThreads(void);
void killRDBThreads(void);
void drainRDBThreadsQueue(void);

ssize_t rdbSaveDbMultiThreaded(rio *rdb, int dbid, long *key_counter, char *pname);
void offloadRDBChunkToThread(
    rio *rdb_main_stream,
    unsigned long chunk_size,
    int rdbver,
    serverDb *current_db,
    int rdbflags,
    pthread_mutex_t *db_insert_mutex,
    int current_dbid,
    long long num_thread_tasks);

#endif // __RDB_THREADS_H__
