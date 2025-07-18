/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "rdb_threads.h"
#include "thread_common.h"

static pthread_t rdb_threads[RDB_THREADS_MAX_NUM] = {0};
static pthread_mutex_t rdb_threads_mutex[RDB_THREADS_MAX_NUM];
JobQueue rdb_jobs[RDB_THREADS_MAX_NUM] = {0};


static void *RDBThreadMain(void *myid) {
    /* The ID is the thread ID number (from 1 to server.rdb_threads_num-1). ID 0 is the main thread. */
    long id = (long)myid;
    char thdname[32];

    snprintf(thdname, sizeof(thdname), "rdb_thd_%ld", id);
    valkey_set_thread_title(thdname);
    // serverSetCpuAffinity(server.rdb_threads_cpulist);

    thread_id = (int)id; // Thread local var defined in thread_common.h
    size_t jobs_to_process = 0;
    JobQueue *jq = &rdb_jobs[id];
    while (1) {
        /* Cancellation point so that pthread_cancel() from main thread is honored. */
        pthread_testcancel();

        /* Wait for jobs */
        for (int j = 0; j < 1000000; j++) {
            jobs_to_process = JobQueue_availableJobs(jq);
            if (jobs_to_process) break;
        }

        /* Give the main thread a chance to stop this thread. */
        if (jobs_to_process == 0) {
            pthread_mutex_lock(&rdb_threads_mutex[id]);
            pthread_mutex_unlock(&rdb_threads_mutex[id]);
            continue;
        }

        for (size_t j = 0; j < jobs_to_process; j++) {
            job_handler handler;
            void *data;
            /* We keep the job in the queue until it's processed. This ensures that if the main thread checks
             * and finds the queue empty, it can be certain that the RDB thread is not currently handling any job. */
            JobQueue_peek(jq, &handler, &data);
            handler(data);
            /* Remove the job after it was processed */
            JobQueue_removeJob(jq);
        }
        /* Memory barrier to make sure the main thread sees the updated tail index.
         * We do it once per loop and not per tail-update for optimization reasons.
         * As the main-thread main concern is to check if the queue is empty, it's enough to do it once at the end. */
        atomic_thread_fence(memory_order_release);
    }
    return NULL;
}

static void createRDBThread(int id, int job_queue_size) {
    serverAssert(server.rdb_threads_num > 0);
    serverAssert(id > 0 && id < server.rdb_threads_num);

    pthread_t tid;
    pthread_mutex_init(&rdb_threads_mutex[id], NULL);
    JobQueue_init(&rdb_jobs[id], job_queue_size);
    // pthread_mutex_lock(&rdb_threads_mutex[id]); /* Thread will be stopped. */
    if (pthread_create(&tid, NULL, RDBThreadMain, (void *)(long)id) != 0) {
        serverLog(LL_WARNING, "Fatal: Can't initialize RDB thread, pthread_create failed with: %s", strerror(errno));
        exit(1);
    }
    rdb_threads[id] = tid;
}

/* Terminates the RDB thread specified by id.
 * Called when the RDB Save or Load has completed*/
static void shutdownRDBThread(int id) {
    int err;
    pthread_t tid = rdb_threads[id];
    serverLog(LL_NOTICE, "Killing thread: %lu", (unsigned long) tid);
    if (tid == pthread_self()) return;
    if (tid == 0) return;
    serverLog(LL_NOTICE, "Sending Cancel to thread: %u", id);

    pthread_cancel(tid);

    if ((err = pthread_join(tid, NULL)) != 0) {
        serverLog(LL_WARNING, "RDB thread(tid:%lu) can not be joined: %s", (unsigned long)tid, strerror(err));
    } else {
        serverLog(LL_NOTICE, "RDB thread(tid:%lu) terminated", (unsigned long)tid);
    }
    serverLog(LL_NOTICE, "Thread: %u canceled", id);

    pthread_mutex_destroy(&rdb_threads_mutex[id]);
    JobQueue_cleanup(&rdb_jobs[id]);
}

void killRDBThreads(void) {
    for (int j = 1; j < server.rdb_threads_num; j++) { /* We don't kill thread 0, which is the main thread. */
        shutdownRDBThread(j);
    }
}

void initRDBThreads(int per_thread_queue_size) {
    if (server.rdb_threads_num == 1) return;

    serverAssert(server.rdb_threads_num <= RDB_THREADS_MAX_NUM);

    /* Spawn and initialize the RDB threads. */
    for (int i = 1; i < server.rdb_threads_num; i++) {
        createRDBThread(i, per_thread_queue_size);
    }
}

/* Main Thread for RDB Save */

static RdbSaveThreadArgs *createRdbSaveThreadArgs(int num_worker_threads, int dbid) {
    RdbSaveThreadArgs *threadArgs = zcalloc(num_worker_threads * sizeof(RdbSaveThreadArgs));

    for (int i = 0; i < num_worker_threads; i++) {
        RdbSaveThreadArgs *ta = &threadArgs[i];
        ta->dbid = dbid;
        ta->bucket_range = (BucketRange){0, 0}; // This is overwritten by the main thread
        ta->ht = NULL;                          // This is set later by the main thread
        atomic_init(&ta->keys_processed, 0);
        atomic_init(&ta->is_done, false);

        ta->worker_buffer = zcalloc(sizeof(RdbSaveWorkerBuffer));

        // Initialize thread buffer and coordination resources
        atomic_init(&ta->worker_buffer->buffer_status, BUFFER_FREE);
        pthread_mutex_init(&ta->worker_buffer->buffer_mutex, NULL);
        pthread_cond_init(&ta->worker_buffer->buffer_cond, NULL);
        rioInitWithBuffer(&ta->worker_buffer->rio, sdsnewlen(SDS_NOINIT, WORKER_BUFFER_SIZE));
    }
    return threadArgs;
}

static void freeRdbSaveThreadArgs(int num_worker_threads, RdbSaveThreadArgs *threadArgs) {
    serverAssert(threadArgs != NULL);

    for (int i = 0; i < num_worker_threads; i++) {
        RdbSaveWorkerBuffer *worker_buffer = threadArgs[i].worker_buffer;

        if (worker_buffer) {
            pthread_mutex_destroy(&worker_buffer->buffer_mutex);
            pthread_cond_destroy(&worker_buffer->buffer_cond);

            // Free the sds buffer associated with the rio instance
            sdsfree(worker_buffer->rio.io.buffer.ptr);
            zfree(worker_buffer);
        }
    }
    zfree(threadArgs);
}

// Calculates a logical bucket range (from 0 -> total_buckets) that a thread will
// responsible for serializing into RDB format
// NOTE: The next iteration will use a mod operation to assign threads buckets. So this will change.
static BucketRange calculateBucketRangeForThread(hashtable *ht, int num_threads, int thread_id) {
    BucketRange range;
    // Step 1: Determine the total number of buckets with 'live data; in the hashtable
    int is_rehashing = hashtableIsRehashing(ht);
    int total_buckets = hashtableBuckets(ht); // This returns  len(tables[0]) + len(tables[0])

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
    RdbSaveWorkerBuffer *wb = args->worker_buffer;
    BucketRange range = args->bucket_range;
    serverDb *db = server.db[args->dbid];

    // serverLog(LL_NOTICE, "Thread ID %d is responsible for buckets [%d, %d)", tid, range.start_index, range.end_index);

    // Step 2: Iterate over all the elements in the hashtable bucket range
    hashtableIterator ht_iter;
    hashtableInitIterator(&ht_iter, ht, HASHTABLE_ITER_PREFETCH_VALUES);
    void *next;

    // Make sure is_done is set to false
    atomic_store(&args->is_done, false);

    bool range_finished = false;
    while (!range_finished) {
        // Loop Phase 1: Wait for buffer to be FREE
        pthread_mutex_lock(&wb->buffer_mutex);
        while (atomic_load(&wb->buffer_status) != BUFFER_FREE) {
            // serverLog(LL_NOTICE, "Thread: %d is waiting for free buffer", tid);
            pthread_cond_wait(&wb->buffer_cond, &wb->buffer_mutex);
        }
        // Loop Phase 2: Fill the buffer with keys from the assigned range.
        // NOTE: The main thread has cleared the buffer so we do not need to do that
        bool buffer_filled_to_threshold = false;
        while (!buffer_filled_to_threshold && !range_finished) {
            if (hashtableRangeNext(&ht_iter, &next, range.start_index, range.end_index) == 0) {
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
            
            size_t processed_bytes_after = wb->rio.processed_bytes;

            // NOTE: We can use res and the processed_bytes_after to determine if we are trying to 
            // buffer a large key. We can then try to just stream the large key directly to the file.
            // If we fail again (becuase we failed for another reason) then we can just fail. 

            if (res != -1) { // increment key count if we succesfully wrote to the buffer
                atomic_fetch_add(&args->keys_processed, 1);
            }
            
            // NOTE: Right now I am assuming this will work for the threads.

            /* In fork child process, we can try to release memory back to the
             * OS and possibly avoid or decrease COW. We give the dismiss
             * mechanism a hint about an estimated size of the object we stored. */
            size_t dump_size = wb->rio.processed_bytes - rdb_bytes_before_key;
            if (server.in_fork_child) dismissObject(o, dump_size);

            unsigned long long threshold = (unsigned long long)WORKER_BUFFER_SIZE * 9 / 10;
            if (wb->rio.processed_bytes >= threshold) {
                buffer_filled_to_threshold = true;
            }

        } // End of inner loop (filling buffer)

        // Phase 3: Signal to main thread that the buffer is READY (if there is data)
        if (wb->rio.processed_bytes > 0) { // Send signal if there is data
            atomic_store(&wb->buffer_status, BUFFER_READY);
            pthread_cond_signal(&wb->buffer_cond);
            // serverLog(LL_DEBUG, "Thread %d: Buffer ready (size %zu), signaled main thread.", tid, wb->rio.processed_bytes);
        }
        pthread_mutex_unlock(&wb->buffer_mutex);
    }

    atomic_store(&args->is_done, true); // Mark this worker as done
    // serverLog(LL_NOTICE, "Thread ID %d: Entire bucket range [%d, %d) processed.", tid, start_index, end_index);
}


static ssize_t processWorkerBuffers(rio *rdb, RdbSaveThreadArgs *threadArgs, int num_worker_threads, long long *info_updated_time, long *last_key_counter, char *pname) {
    ssize_t written = 0;
    ssize_t res;

    bool all_workers_done_for_hashtable;
    bool any_buffer_ready_to_write;

    do {
        all_workers_done_for_hashtable = true;
        any_buffer_ready_to_write = false;

        // Check if all the workers are completely done
        for (int i = 0; i < num_worker_threads; i++) {
            if (!atomic_load(&threadArgs[i].is_done)) {
                all_workers_done_for_hashtable = false;
            }

            RdbSaveWorkerBuffer *wb = threadArgs[i].worker_buffer;
            // Check if this worker's buffer is ready for us to write to dump.rdb
            pthread_mutex_lock(&wb->buffer_mutex);
            // serverLog(LL_NOTICE, "Main Thread: got mutex for buffer: %d", i);
            if (atomic_load(&wb->buffer_status) == BUFFER_READY) {
                any_buffer_ready_to_write = true;
                // serverLog(LL_NOTICE, "Main Thread: Worker Buffer %d is READY with len %ld", i, sdslen(wb->rio.io.buffer.ptr));

                // Write out the workers buffer content to the main RDB file stream
                size_t bytes_to_write = sdslen(wb->rio.io.buffer.ptr);
                if (bytes_to_write > 0) {
                    if ((res = rdbWriteRaw(rdb, wb->rio.io.buffer.ptr, bytes_to_write)) == -1) {
                        serverLog(LL_WARNING, "Main thread: Failed to write worker buffer %d to main RDB file.", i);
                        return C_ERR;
                    }
                }
                written += res;

                // Clear the buffer now that we have processed it
                sdsclear(wb->rio.io.buffer.ptr);
                wb->rio.processed_bytes = 0;
                atomic_store(&wb->buffer_status, BUFFER_FREE);
                pthread_cond_signal(&wb->buffer_cond); // Signal for the thread to wake up
            }
            // serverLog(LL_NOTICE, "Main Thread: Releaseing mutex for buffer: %d", i);
            pthread_mutex_unlock(&wb->buffer_mutex);
        }
        long total_keys_processed = 0;
        for (int i = 0; i < num_worker_threads; i++) {
            long keys_processed_by_thread = atomic_load(&threadArgs[i].keys_processed);
            total_keys_processed += keys_processed_by_thread;
        }
        /* Update child info every 1 second (approximately).
         * in order to avoid calling mstime() on each iteration, we will
         * check the time diff if we have processed more than 1023 keys */
        if ((total_keys_processed - *last_key_counter) > 1023) {
            long long now = mstime();
            if (now - *info_updated_time >= 1000) {
                // Update the key counter and notify the parent process
                *last_key_counter = total_keys_processed;
                sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, *last_key_counter, pname);
                *info_updated_time = now;
            }
        }

    } while (!all_workers_done_for_hashtable || any_buffer_ready_to_write);
    // This loop continues as long as:
    // 1: There is at least one worker with 'is_done' = False (i.e still processing data)
    // 2: There are still buffers ready to be written out.

    // // After the main loop, there's a final potential state where all workers are done
    // // but a buffer became ready right at the end or was not yet processed.
    // // This loop ensures that all remaining READY buffers are processed.
    // // We loop as long as *any* buffer is found READY in an iteration.
    // do {
    //     any_buffer_ready_to_write = false;
    //     for (int i = 0; i < num_worker_threads; i++) {
    //         RdbSaveWorkerBuffer *wb = threadArgs[i].worker_buffer;
    //         pthread_mutex_lock(&wb->buffer_mutex);
    //         if (atomic_load(&wb->buffer_status) == BUFFER_READY) {
    //             any_buffer_ready_to_write = true; // Found another ready buffer, so we need another pass
    //             size_t bytes_to_write = sdslen(wb->rio.io.buffer.ptr);
    //             if (bytes_to_write > 0) {
    //                 if (rdbWriteRaw(rdb, wb->rio.io.buffer.ptr, bytes_to_write) == -1) {
    //                     serverLog(LL_WARNING, "Main thread: Failed to write worker buffer %d to main RDB file.", i);
    //                 }
    //             }
    //             sdsclear(wb->rio.io.buffer.ptr);
    //             wb->rio.processed_bytes = 0;
    //             atomic_store(&wb->buffer_status, BUFFER_FREE);
    //             pthread_cond_signal(&wb->buffer_cond); // Signal back to worker (though it might be done)
    //         }
    //         pthread_mutex_unlock(&wb->buffer_mutex);
    //     }
    // } while (any_buffer_ready_to_write); // Keep looping as long as we processed any buffer in the last pass

    // Final key count (redundant with the one at the end, but good for safety)
    long total_keys_processed = 0;
    for (int i = 0; i < num_worker_threads; i++) {
        total_keys_processed += atomic_load(&threadArgs[i].keys_processed);
    }
    serverLog(LL_NOTICE, "Process Hashtable completed. Num Keys Saved: %lu", total_keys_processed);

    *last_key_counter = total_keys_processed;
    
    return written;
}

ssize_t rdbSaveDbMultiThreaded(rio *rdb, int dbid, long *key_counter, char *pname) {
    serverAssert(server.rdb_threads_num > 1);
    ssize_t written = 0;

    int num_rdb_save_worker_threads = server.rdb_threads_num - 1;
    serverLog(LL_NOTICE, "rdbSaveDbMultiThreaded: Saving DB to disk using %d worker threads and 1 write thread", num_rdb_save_worker_threads);


    serverDb *db = server.db[dbid];
    long long info_updated_time = 0;

    // Step 1: Create the base tasks that will be passed to the threads
    RdbSaveThreadArgs *threadArgs = createRdbSaveThreadArgs(num_rdb_save_worker_threads, dbid);

    // Step 2: Iterate through the hashtables (slots) in the kvstore (in standalone mode there is 1 hashtable)
    kvstoreIterator *kvs_it = kvstoreIteratorInit(db->keys, HASHTABLE_ITER_SAFE | HASHTABLE_ITER_PREFETCH_VALUES);
    hashtable *ht;
    int last_slot = -1;

    while ((ht = kvstoreIteratorNextHashtable(kvs_it)) != NULL) {
        // Ensure rehashing is paused
        // hashtablePauseRehashing(ht);

        // Step 2a: For each hashtable, write the metadata to the RDB file (if we are in cluster mode)
        int curr_slot = kvstoreIteratorGetCurrentHashtableIndex(kvs_it);
        /* Save slot info. */
        if (server.cluster_enabled && curr_slot != last_slot) {
            sds slot_info = sdscatprintf(sdsempty(), "%i,%lu,%lu", curr_slot,
                                         kvstoreHashtableSize(db->keys, curr_slot),
                                         kvstoreHashtableSize(db->expires, curr_slot));
            rdbSaveAuxFieldStrStr(rdb, "slot-info", slot_info);
            sdsfree(slot_info);
            last_slot = curr_slot;
        }

        // Step 3b: Allocate a range of the hashtable to each RDB thread
        // Note: i = 0 represents the main thread
        for (int thread_id = 1; thread_id < server.rdb_threads_num; thread_id++) { 
            RdbSaveThreadArgs *ta = &threadArgs[thread_id-1];
            RdbSaveWorkerBuffer *wb = ta->worker_buffer;
            pthread_mutex_lock(&wb->buffer_mutex);
            ta->ht = ht;
            // Note: calculateBucket range expects thread_ids to start at index 0
            ta->bucket_range = calculateBucketRangeForThread(ht, num_rdb_save_worker_threads, thread_id-1); 
            atomic_store(&ta->is_done, false); // Mark task as not done
            sdsclear(wb->rio.io.buffer.ptr);   // Make sure the buffer is clear
            wb->rio.processed_bytes = 0;
            atomic_store(&wb->buffer_status, BUFFER_FREE);
            pthread_mutex_unlock(&wb->buffer_mutex);

            // Get the threads job queue and add a new rdb encoding job
            JobQueue *jq = &rdb_jobs[thread_id];
            if (JobQueue_isFull(jq)) {
                serverLog(LL_WARNING, "This should not happen...");
            }
            JobQueue_push(jq, rdbEncodeHashtableRange, ta);
        }

        // Step 2c: Poll the threads buffers and write them out to rdb file
        // Once the hash table has been written out entirely we will continue to the next iteration
        written += processWorkerBuffers(rdb, threadArgs, num_rdb_save_worker_threads, &info_updated_time, key_counter, pname);
        // hashtableResumeRehashing(ht);
    }
    kvstoreIteratorRelease(kvs_it);
    freeRdbSaveThreadArgs(num_rdb_save_worker_threads, threadArgs);
    serverLog(LL_NOTICE, "rdbSaveHashtablesMultithreaded completed. Num Keys Saved: %lu", *key_counter);
    return written;
}
