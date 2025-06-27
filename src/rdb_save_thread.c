#include "rdb_save_thread.h"
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include "rdb.h"

RdbSaveThreadArgs *createRdbSaveThreadArgs(int num_threads, int dbid) {
    serverAssert(num_threads > 0);

    RdbSaveThreadArgs *threadArgs = zcalloc(num_threads * sizeof(RdbSaveThreadArgs));
    // Should I handle failed memory allocation?
    // Initialize the RdbSaveThreadArgs and its WorkerBuffer
    for (int i = 0; i < num_threads; i++) {
        RdbSaveThreadArgs *ta = &threadArgs[i];
        ta->thread_id = i;
        ta->dbid = dbid;
        ta->bucket_range = (BucketRange){0, 0}; // This is overwritten by the main thread
        ta->ht = NULL; // This is set later in the main thread
        atomic_init(&ta->keys_processed, 0);
        atomic_init(&ta->is_done, false);

        ta->worker_buffer = zcalloc(sizeof(WorkerBuffer));
        // Should I handle failed memory allocation?

        atomic_init(&ta->worker_buffer->buffer_status, BUFFER_FREE); 
        pthread_mutex_init(&ta->worker_buffer->buffer_mutex, NULL); 
        pthread_cond_init(&ta->worker_buffer->buffer_cond, NULL);
        rioInitWithBuffer(&ta->worker_buffer->rio, sdsnewlen(SDS_NOINIT, WORKER_BUFFER_SIZE));
    }
    return threadArgs;
}

void freeRdbSaveThreadArgs(int num_threads, RdbSaveThreadArgs *threadArgs) {
    serverAssert(threadArgs != NULL);

    for (int i = 0; i < num_threads; i++) {
        WorkerBuffer *wb = threadArgs[i].worker_buffer;

        if (wb) {
            pthread_mutex_destroy(&wb->buffer_mutex);
            pthread_cond_destroy(&wb->buffer_cond);

            // Free the sds buffer associated with the rio instance
            // I am pretty sure we need to do this but not 100%
            sdsfree(wb->rio.io.buffer.ptr);
            zfree(wb);
        }
    }
    zfree(threadArgs);
}

// Calculates a logical bucket range (from 0 -> total_buckets) that a thread will 
// responsible for serializing into RDB format
BucketRange calculateBucketRangeForThread(hashtable *ht, int num_threads, int thread_id) {
    BucketRange range;
    // Step 1: Determine the total number of buckets with 'live data; in the hashtable
    int is_rehashing = hashtableIsRehashing(ht);
    int total_buckets = hashtableBuckets(ht);
    
    if (is_rehashing) {
        // The buckets that have been rehashed are not included in the total count
        total_buckets = total_buckets - hashtableRehashIndex(ht);
    }
    
    int base_buckets_per_thread = total_buckets / num_threads;
    int remaining_buckets = total_buckets % num_threads;

    // Step 2: Calculate the bucket range for this thread
    // if this thread is one of the first 'remaining_buckets' threads
    // it will get one extra bucket
    if (thread_id < remaining_buckets) {
        range.start_index = thread_id * (base_buckets_per_thread + 1);
        range.end_index = range.start_index + base_buckets_per_thread + 1;
    } else {
        range.start_index = thread_id * base_buckets_per_thread + remaining_buckets;
        range.end_index = range.start_index + base_buckets_per_thread;
    }

    return range;
}

void rdbEncodeHashtableRange(void *arg) {
    // Step 1: Extract the thread arguments (including the hashtable and worker buffer)
    RdbSaveThreadArgs *args = (RdbSaveThreadArgs *)arg;
    hashtable *ht = args->ht;
    BucketRange range = args->bucket_range;
    WorkerBuffer *wb = args->worker_buffer;
    int tid = args->thread_id;
    serverDb *db = server.db + args->dbid;
    // serverLog(LL_NOTICE, "Thread ID %d is responsible for buckets [%d, %d)", tid, range.start_index, range.end_index);
    
    // Step 3: Iterate over all the elements in the hashtable bucket range
    hashtableIterator ht_iter;
    hashtableInitRangeIterator(&ht_iter, ht, range.start_index);
    void *next;

    // Make sure is_done is set to false
    atomic_store(&args->is_done, false);
    
    bool range_finished = false;
    while (!range_finished) {
        // Loop Phase 1: Wait for buffer to be FREE
        pthread_mutex_lock(&wb->buffer_mutex);
        while(atomic_load(&wb->buffer_status) != BUFFER_FREE) {
            serverLog(LL_NOTICE, "Thread: %d is waiting for free buffer", tid);
            pthread_cond_wait(&wb->buffer_cond, &wb->buffer_mutex);
        }
        // Loop Phase 2: Fill the buffer with keys from the assigned range. 
        // NOTE: The main thread has cleared the buffer so we do not need to do that
        bool buffer_filled_to_threshold = false; 
        while(!buffer_filled_to_threshold && !range_finished) {
            if (hashtableRangeNext(&ht_iter, &next, range.end_index) == 0) {
                range_finished = true;
                break;
            }

            robj *o = next;
            sds keystr = objectGetKey(o);
            robj key;
            long long expire;
            size_t rdb_bytes_before_key = wb->rio.processed_bytes;
            
            initStaticStringObject(key, keystr);
            expire = getExpire(db, &key);
            
            ssize_t res;
            res = rdbSaveKeyValuePair(&wb->rio, &key, o, expire, args->dbid);

            
            if (res != -1) { // increment key count if we succesfully wrote to the buffer
                atomic_fetch_add(&args->keys_processed, 1); 
            }

            
            // NOTE: This seemed to be causing some errors. MUST LOOK DEEPER INTO THIS.
            /* In fork child process, we can try to release memory back to the
            * OS and possibly avoid or decrease COW. We give the dismiss
            * mechanism a hint about an estimated size of the object we stored. */
            size_t dump_size = wb->rio.processed_bytes - rdb_bytes_before_key;
            if (server.in_fork_child) dismissObject(o, dump_size);
            

            if (wb->rio.processed_bytes >= (WORKER_BUFFER_SIZE)) {
                buffer_filled_to_threshold = true;
            }

            serverLog(LL_NOTICE, "Thread: %d saving key: %s, res =%ld", tid, keystr, res);
            size_t processed_bytes_after = wb->rio.processed_bytes;
            serverLog(LL_NOTICE, "Thread: %d rdb_bytes_before_key: %lu, wb->rio.processed_bytes: %lu", tid, rdb_bytes_before_key, processed_bytes_after);

        } // End of inner loop (filling buffer)

        // Phase 3: Signal to main thread that the buffer is READY (if there is data)
        if (wb->rio.processed_bytes > 0) { // Send signal if there is data
            atomic_store(&wb->buffer_status, BUFFER_READY);
            pthread_cond_signal(&wb->buffer_cond);
            serverLog(LL_DEBUG, "Thread %d: Buffer ready (size %zu), signaled main thread.", tid, wb->rio.processed_bytes);
        }
        pthread_mutex_unlock(&wb->buffer_mutex);

    }
    atomic_store(&args->is_done, true); // Mark this worker as done
    // serverLog(LL_NOTICE, "Thread ID %d: Entire bucket range [%d, %d) processed.", tid, start_index, end_index);
}