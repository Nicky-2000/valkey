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
    pthread_mutex_lock(&rdb_threads_mutex[id]); /* Thread will be stopped. */
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
    if (tid == pthread_self()) return;
    if (tid == 0) return;

    pthread_cancel(tid);

    if ((err = pthread_join(tid, NULL)) != 0) {
        serverLog(LL_WARNING, "RDB thread(tid:%lu) can not be joined: %s", (unsigned long)tid, strerror(err));
    } else {
        serverLog(LL_NOTICE, "RDB thread(tid:%lu) terminated", (unsigned long)tid);
    }
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
