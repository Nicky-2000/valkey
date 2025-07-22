/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "rdb_threads.h"
#include "thread_common.h"

static pthread_t rdb_threads[RDB_THREADS_MAX_NUM] = {0};
static pthread_mutex_t rdb_threads_mutex[RDB_THREADS_MAX_NUM];
JobQueue rdb_jobs[RDB_THREADS_MAX_NUM] = {0}; // Job queues for each RDB worker thread.


/* --------- RDB Worker Threads Core Logic --------- */

static void *RDBThreadMain(void *myid) {
    /* The ID is the thread ID number (from 1 to server.rdb_threads_num-1). ID 0 is the main thread. */
    long id = (long)myid;
    char thdname[32];

    snprintf(thdname, sizeof(thdname), "rdb_thd_%ld", id);
    valkey_set_thread_title(thdname);
    
    /* 
        Note: CPU Affinity for rdb save cab be added here using:
        'serverSetCpuAffinity(server.rdb_threads_cpulist)'
    */

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


/* --------- RDB Threads Lifecycle Management --------- */

/* We only need a small queue for RDB Save because we processes hashtables sequentially.
 * RDB Load may need larger queues. */
static void createRDBThread(int id, int job_queue_size) {
    serverAssert(server.rdb_threads_num > 0);
    serverAssert(id > 0 && id < server.rdb_threads_num);

    pthread_t tid;
    pthread_mutex_init(&rdb_threads_mutex[id], NULL);
    JobQueue_init(&rdb_jobs[id], job_queue_size);
    pthread_mutex_lock(&rdb_threads_mutex[id]); /* Thread starts paused */
    if (pthread_create(&tid, NULL, RDBThreadMain, (void *)(long)id) != 0) {
        serverLog(LL_WARNING, "Fatal: Can't initialize RDB thread, pthread_create failed with: %s", strerror(errno));
        exit(1);
    }
    rdb_threads[id] = tid;
}

/* Terminates the RDB thread specified by id */
static void shutdownRDBThread(int id) {
    int err;
    pthread_t tid = rdb_threads[id];
    if (tid == pthread_self()) return;
    if (tid == 0) return;    
    pthread_mutex_unlock(&rdb_threads_mutex[id]);

    pthread_cancel(tid);

    if ((err = pthread_join(tid, NULL)) != 0) {
        serverLog(LL_WARNING, "RDB thread(tid:%lu) can not be joined: %s", (unsigned long)tid, strerror(err));
    } else {
        serverLog(LL_NOTICE, "RDB thread(tid:%lu) terminated", (unsigned long)tid);
    }
    pthread_mutex_destroy(&rdb_threads_mutex[id]);
    JobQueue_cleanup(&rdb_jobs[id]);
}

/* Terminates all RDB Worker Threads. Called when RDB Save or Load has completed */
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


/* --------- Multithreaded RDB Save: Thread Argument Management --------- */

static RdbSaveThreadArgs *createRdbSaveThreadArgs(int num_threads, int dbid, rio *rdb, long *key_counter, char *pname, long long * info_updated_time) {
    RdbSaveThreadArgs *threadArgs = zcalloc(num_threads * sizeof(RdbSaveThreadArgs));
    pthread_mutex_t *shared_write_mutex = zmalloc(sizeof(pthread_mutex_t)); // Shared amongst all threads
    pthread_mutex_init(shared_write_mutex, NULL);

    for (int i = 0; i < num_threads; i++) {
        RdbSaveThreadArgs *ta = &threadArgs[i];
        ta->dbid = dbid;
        ta->ht = NULL;  // Set by the main thread in rdbSaveDbMultiThreaded for each hashtable in the database
        ta->bucket_stride = (BucketStride){.start_index=i, .stride_size=num_threads};
        atomic_init(&ta->keys_processed, 0);
        rioInitWithMemCappedBuffer(&ta->memcap_buffer_rio, sdsnewlen(SDS_NOINIT, WORKER_BUFFER_DEFAULT_SIZE), WORKER_BUFFER_CAPACITY_LIMIT);
        ta->rdb = rdb;
        ta->write_mutex = shared_write_mutex;
        ta->save_status = C_OK;
        
        /* The main thread needs this information to report the save progress */
        if (i == 0) {
            ta->main_thread_report_info = zcalloc(sizeof(MainThreadRdbInfo));
            ta->main_thread_report_info->info_updated_time = info_updated_time;
            ta->main_thread_report_info->last_key_counter = key_counter;
            ta->main_thread_report_info->pname = pname;
            ta->main_thread_report_info->threadArgs = threadArgs;

        } else{
            ta->main_thread_report_info = NULL;
        }
    }
    return threadArgs;
}

static void freeRdbSaveThreadArgs(int num_threads, RdbSaveThreadArgs *threadArgs) {
    serverAssert(threadArgs != NULL);

    for (int i = 0; i < num_threads; i++) {
        sdsfree(threadArgs[i].memcap_buffer_rio.io.memcap_buffer.ptr);
    }
    /* Free the shared write mutex one time*/
    pthread_mutex_t *shared_write_mutex = threadArgs[0].write_mutex;
    pthread_mutex_destroy(shared_write_mutex);
    zfree(shared_write_mutex);
    
    /* Free the MainThreadRdbInfo */
    zfree(threadArgs[0].main_thread_report_info);

    zfree(threadArgs);
}


/* --------- Multithreaded RDB Save: Worker Job Handler & Helpers --------- */

/* Clears and resets a memory-capped RIO buffer. */
void clearRioMemCapBuffer(rio* memcap_buffer_rio) {
    sdsclear(memcap_buffer_rio->io.memcap_buffer.ptr);
    memcap_buffer_rio->io.memcap_buffer.cap_reached = 0;
    memcap_buffer_rio->io.memcap_buffer.pos = 0;
    memcap_buffer_rio->processed_bytes = 0;
}

/* Updates the parent process with RDB save progress.
 * Updates are batched to reduce overhead: approximately every 1024 keys or 1 second. */
void updateParentProcessWithSaveInfo(MainThreadRdbInfo *reporting_info) {
    long total_keys_processed = 0;
    for (int i = 0; i < server.rdb_threads_num; i++) {
        total_keys_processed += atomic_load(&reporting_info->threadArgs[i].keys_processed);
    }

    /* Update child info periodically to avoid excessive `mstime()` calls and parent notifications. */
    if ((total_keys_processed - *reporting_info->last_key_counter) > 1023) {
        long long now = mstime();
        if (now - *reporting_info->info_updated_time >= 1000) {
            *reporting_info->last_key_counter = total_keys_processed;
            sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, total_keys_processed, reporting_info->pname);
            *reporting_info->info_updated_time = now;
        }
    }
}

/* Job handler for RDB worker threads: encodes a range of hashtable buckets. */
void rdbEncodeHashtableRange(void *arg) {
    /* Step 1: Extract the thread arguments */
    RdbSaveThreadArgs *args = (RdbSaveThreadArgs *)arg;
    serverDb *db = server.db[args->dbid];
    hashtable *ht = args->ht;
    BucketStride *bucket_stride = &args->bucket_stride;
    rio *memcap_buffer_rio = &args->memcap_buffer_rio;
    
    hashtableIterator ht_iter;
    hashtableInitIterator(&ht_iter, ht, HASHTABLE_ITER_PREFETCH_VALUES);
    void *next;
    ssize_t res;
    
    /* Iterate through hashtable buckets assigned to this thread and encode keys/values. */
    while (hashtableStrideNext(&ht_iter, &next, bucket_stride->start_index, bucket_stride->stride_size)) {
        robj *o = next;
        sds keystr = objectGetKey(o);
        robj key;
        long long expire;
        size_t processed_bytes_before = memcap_buffer_rio->processed_bytes;

        initStaticStringObject(key, keystr);
        expire = getExpire(db, &key);

        /* Attempt to write key-value pair to the thread's local buffer. */
        res = rdbSaveKeyValuePair(memcap_buffer_rio, &key, o, expire, args->dbid);
        size_t processed_bytes_after = memcap_buffer_rio->processed_bytes;

        if (res < 0 && memcap_buffer_rio->io.memcap_buffer.cap_reached) { /* Failed write due to hitting the buffers memory cap */
            /* If key is too large for the buffer, we will write the valid data in the buffer to the rdb file
               and the stream the large key directly to the rdb. */
            pthread_mutex_lock(args->write_mutex);

            if (processed_bytes_before > 0) {
                /* If there are already encoded keys in the buffer we can also write them out */
                if ((res = rdbWriteRaw(args->rdb, memcap_buffer_rio->io.memcap_buffer.ptr, processed_bytes_before)) < 0) goto werr;
                args->bytes_written += res;
                clearRioMemCapBuffer(memcap_buffer_rio);
            }

            /* Stream large key directly to RDB, bypassing the buffer. */
            processed_bytes_before = args->rdb->processed_bytes;
            if ((res = rdbSaveKeyValuePair(args->rdb, &key, o, expire, args->dbid)) < 0) goto werr;
            processed_bytes_after = args->rdb->processed_bytes;
            args->bytes_written += res;

            pthread_mutex_unlock(args->write_mutex);
        } else if (res < 0) {
            goto werr; /* Write failed for some reason other than exceeding the buffer limit */
        }

        atomic_fetch_add(&args->keys_processed, 1);

        /* In fork child process, we can try to release memory back to the
        * OS and possibly avoid or decrease COW. We give the dismiss
        * mechanism a hint about an estimated size of the object we stored. */
        size_t dump_size = processed_bytes_after - processed_bytes_before;
        if (server.in_fork_child) dismissObject(o, dump_size);

        /* If we have filled up the buffer aquire lock and write contents to RDB file */
        if (memcap_buffer_rio->processed_bytes > (size_t) WORKER_BUFFER_DEFAULT_SIZE) {
            pthread_mutex_lock(args->write_mutex);
            if ((res = rdbWriteRaw(args->rdb, memcap_buffer_rio->io.memcap_buffer.ptr, memcap_buffer_rio->processed_bytes)) < 0) goto werr;
            pthread_mutex_unlock(args->write_mutex);

            args->bytes_written += res;
            clearRioMemCapBuffer(memcap_buffer_rio);
        }

        /* Main thread only: update parent process with progress. */
        if (inMainThread()) {
            updateParentProcessWithSaveInfo(args->main_thread_report_info);
        }
    }

    /* Write any remaining buffered data to the RDB file. */
    if (memcap_buffer_rio->processed_bytes > 0) {
        pthread_mutex_lock(args->write_mutex);
        if ((res = rdbWriteRaw(args->rdb, memcap_buffer_rio->io.memcap_buffer.ptr, memcap_buffer_rio->processed_bytes)) < 0) goto werr;
        pthread_mutex_unlock(args->write_mutex);

        args->bytes_written += res;
        clearRioMemCapBuffer(memcap_buffer_rio);
    }

    hashtableResetIterator(&ht_iter);
    return;

werr:
    hashtableResetIterator(&ht_iter);
    args->save_status = C_ERR;
    serverLog(LL_WARNING, "RDB thread (%d): Failed to write buffer to rdb file.", getThreadID());
}


/* --------- Multithreaded RDB Save: Main Thread Orchestration --------- */

/* Drains all RDB thread queues, ensuring all jobs are processed before proceeding.
 * Must be called from the main thread. */
void drainRDBThreadsQueue(void) {
    serverAssert(inMainThread());
    for (int i = 1; i < RDB_THREADS_MAX_NUM; i++) { /* No need to drain thread 0, which is the main thread. */
        while (!JobQueue_isEmpty(&rdb_jobs[i])) {
            /* memory barrier acquire to get the latest job queue state */
            atomic_thread_fence(memory_order_acquire);
        }
    }
}

/* Performs a multithreaded RDB save for a specific database. */
ssize_t rdbSaveDbMultiThreaded(rio *rdb, int dbid, long *key_counter, char *pname) {
    serverAssert(server.rdb_threads_num > 1);
    ssize_t written = 0;

    serverDb *db = server.db[dbid];
    long long info_updated_time = 0;

    /* 1. Create and initialize thread arguments for all RDB threads. */
    RdbSaveThreadArgs *threadArgs = createRdbSaveThreadArgs(server.rdb_threads_num, dbid, rdb, key_counter, pname, &info_updated_time);

    kvstoreIterator *kvs_it = kvstoreIteratorInit(db->keys, HASHTABLE_ITER_SAFE | HASHTABLE_ITER_PREFETCH_VALUES);
    hashtable *ht;
    int last_slot = -1;

    /* 2. Iterate through the hashtables (slots) in the kvstore (in standalone mode there is 1 hashtable). */
    while ((ht = kvstoreIteratorNextHashtable(kvs_it)) != NULL) {
        /* 2.1. Write metadata (e.g., "slot-info") to RDB file if in cluster mode. */
        int curr_slot = kvstoreIteratorGetCurrentHashtableIndex(kvs_it);
        if (server.cluster_enabled && curr_slot != last_slot) {
            sds slot_info = sdscatprintf(sdsempty(), "%i,%lu,%lu", curr_slot,
                                         kvstoreHashtableSize(db->keys, curr_slot),
                                         kvstoreHashtableSize(db->expires, curr_slot));
            rdbSaveAuxFieldStrStr(rdb, "slot-info", slot_info);
            sdsfree(slot_info);
            last_slot = curr_slot;
        }

        /* 2.2. Assign a range of the current hashtable to each RDB thread. */
        for (int i = 0; i < server.rdb_threads_num; i++) { 
            RdbSaveThreadArgs *ta = &threadArgs[i];
            ta->ht = ht;
            clearRioMemCapBuffer(&ta->memcap_buffer_rio);

            if (i == 0) continue; /* Main thread processes its job directly. Don't need to queue. */ 
            
            JobQueue *jq = &rdb_jobs[i];
            if (JobQueue_isFull(jq)) goto werr;
            JobQueue_push(jq, rdbEncodeHashtableRange, ta);
            pthread_mutex_unlock(&rdb_threads_mutex[i]); /* Unlock thread now that it has a job */
        }
        /* Main thread processes its portion of the hashtable. */
        rdbEncodeHashtableRange(&threadArgs[0]);
        
        /* 2.3. Wait for all threads to complete their jobs. */
        drainRDBThreadsQueue();
        
        /* 2.4. Pause worker threads until their next job assignment. */
        for (int i = 1; i < server.rdb_threads_num; i++) {
            pthread_mutex_lock(&rdb_threads_mutex[i]);
        }

        /* 2.5. Check for errors reported by any thread. */
        for (int i = 0; i < server.rdb_threads_num; i++) {
            if (threadArgs[i].save_status == C_ERR) goto werr; 
        }
    }

    /* 4. Aggregate total bytes written and keys processed from all threads. */
    long long total_keys_written = 0;
    for (int i = 0; i < server.rdb_threads_num; i++) {
        written += threadArgs[i].bytes_written;
        total_keys_written += atomic_load(&threadArgs[i].keys_processed);
    }

    kvstoreIteratorRelease(kvs_it);
    freeRdbSaveThreadArgs(server.rdb_threads_num, threadArgs);
    serverLog(LL_DEBUG, "rdbSaveHashtablesMultithreaded completed. Num Keys Saved: %llu", total_keys_written);
    return written;

werr:
    kvstoreIteratorRelease(kvs_it);
    freeRdbSaveThreadArgs(server.rdb_threads_num, threadArgs);
    return -1;
}
