#include "rdb_save_thread.h"
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include "rdb.h"


static BucketRange calculateBucketRange(int total_buckets, int num_threads, int thread_id) {
    BucketRange range;

    int base_buckets_per_thread = total_buckets / num_threads;
    int remaining_buckets = total_buckets % num_threads;

    // Distribute buckets: give one extra bucket to the first 'remaining_buckets' threads
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
    WorkerBuffer *wb = args->worker_buffer;
    int tid = args->thread_id;
    serverDb *db = server.db + args->dbid;

    // Step 2: Use the hashtable, and the thread id to determine
    // the range of buckets this thread will process
    int is_rehashing = hashtableIsRehashing(ht);
    int total_buckets = hashtableBuckets(ht);
    if (is_rehashing) {
        // The buckets that have been rehashed are not included in the total count
        total_buckets = total_buckets - hashtableRehashIndex(ht);
    }

    BucketRange thread_bucket_range = calculateBucketRange(total_buckets, args->num_threads, tid);
    int start_index = thread_bucket_range.start_index;
    int end_index = thread_bucket_range.end_index;

    serverLog(LL_NOTICE, "Thread ID %d is responsible for buckets [%d, %d)", tid, start_index, end_index);
    
    // Step 3: Iterate over all the elements in the hashtable bucket range
    hashtableIterator ht_iter;
    hashtableInitRangeIterator(&ht_iter, ht, start_index);
    void *next;

    // Make sure is_done is set to false
    atomic_store(&args->is_done, false);
    
    bool range_finished = false;
    while (!range_finished) {
        // Loop Phase 1: Wait for buffer to be FREE
        serverLog(LL_NOTICE, "Thread: %d trying to get mutex", tid);
        pthread_mutex_lock(&wb->buffer_mutex);
        while(atomic_load(&wb->buffer_status) != BUFFER_FREE) {
            serverLog(LL_NOTICE, "Thread: %d is waiting for free buffer", tid);
            pthread_cond_wait(&wb->buffer_cond, &wb->buffer_mutex);
        }
        // Loop Phase 2: Fill the buffer with keys from the assigned range. 
        // NOTE: The main thread has cleared the buffer so we do not need to do that
        bool buffer_filled_to_threshold = false; 
        while(!buffer_filled_to_threshold && !range_finished) {
            if (hashtableRangeNext(&ht_iter, &next, end_index) == 0) {
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


            /* In fork child process, we can try to release memory back to the
            * OS and possibly avoid or decrease COW. We give the dismiss
            * mechanism a hint about an estimated size of the object we stored. */
            size_t dump_size = wb->rio.processed_bytes - rdb_bytes_before_key;
            if (server.in_fork_child) dismissObject(o, dump_size);
            
            // WILL NEED TO UPDATE THIS LATER.. 1 thread should do the update!
            /* Update child info every 1 second (approximately).
            * in order to avoid calling mstime() on each iteration, we will
            * check the diff every 1024 keys */
            // if (((*key_counter)++ & 1023) == 0) {
            //     long long now = mstime();
            //     if (now - info_updated_time >= 1000) {
            //         sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, *key_counter, pname);
            //         info_updated_time = now;
            //     }
            // }

            if (wb->rio.processed_bytes >= (WORKER_BUFFER_SIZE)) {
                buffer_filled_to_threshold = true;
            }
        } // End of inner loop (filling buffer)

        // Phase 3: Signal to main thread that the buffer is READY (if there is data)
        pthread_mutex_lock(&wb->buffer_mutex);
        if (wb->rio.processed_bytes > 0) { // Send signal if there is data
            atomic_store(&wb->buffer_status, BUFFER_READY);
            pthread_cond_signal(&wb->buffer_cond);
            serverLog(LL_DEBUG, "Thread %d: Buffer ready (size %zu), signaled main thread.", tid, wb->rio.processed_bytes);
        }
    }
    atomic_store(&args->is_done, true); // Mark this worker as done
    serverLog(LL_NOTICE, "Thread ID %d: Entire bucket range [%d, %d) processed.", tid, start_index, end_index);
}