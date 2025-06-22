#include "rdb_parallel_batch.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>

// This function is the entry point for each worker thread.
// Here we will wait for a signal to start processing from the main thread.
// Then we will signal that we are done processing.
// When all threads are done processing, the main thread will set up the next batch 
// of work and signal the threads to start again.
static void *worker_entry(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;

    while (1) {
        // Wait for start signal
        pthread_mutex_lock(args->start_mutex);
        while (!atomic_load(args->start_flag)) {
            pthread_cond_wait(args->start_cond, args->start_mutex);
        }
        pthread_mutex_unlock(args->start_mutex);

        // Reset completion state
        WorkerBuffer *wb = args->worker_buffer;
        atomic_store(&wb->is_done, false);

        // Run thread-specific work
        args->work_fn(arg);

        // Mark this thread as done
        atomic_store(&wb->is_done, true);
    }

    return NULL;
}

// Here we create the parallel batch context
// This creates the threads and their buffers 
// The context contains all the information the main thread needs to manage the worker threads.
// It also contains the worker buffers that the threads will use to write their results too.
ParallelBatchContext *createParallelBatchContext(int num_threads, size_t buffer_size, void (*work_fn)(void *)) {
    ParallelBatchContext *ctx = malloc(sizeof(ParallelBatchContext));
    ctx->num_threads = num_threads;
    ctx->threads = malloc(sizeof(pthread_t) * num_threads);
    ctx->thread_args = calloc(num_threads, sizeof(ThreadArgs));
    ctx->worker_buffers = calloc(num_threads, sizeof(WorkerBuffer));

    pthread_mutex_init(&ctx->start_mutex, NULL);
    pthread_cond_init(&ctx->start_cond, NULL);
    atomic_init(&ctx->start_flag, false);
    ctx->work_fn = work_fn;

    for (int i = 0; i < num_threads; ++i) {
        WorkerBuffer *wb = &ctx->worker_buffers[i];
        for (int j = 0; j < NUM_BUFFERS; ++j) {
            wb->buffers[j] = malloc(buffer_size);
            atomic_init(&wb->status[j], BUFFER_FREE);
        }
        wb->current_write_index = 0;
        atomic_init(&wb->is_done, false);
        pthread_mutex_init(&wb->mutex, NULL);
        pthread_cond_init(&wb->cond, NULL);

        ThreadArgs *arg = &ctx->thread_args[i];
        *arg = (ThreadArgs){
            .thread_id = i,
            .num_threads = num_threads,
            .start_cond = &ctx->start_cond,
            .start_mutex = &ctx->start_mutex,
            .start_flag = &ctx->start_flag,
            .worker_buffer = wb,
            .work_fn = ctx->work_fn
        };

        pthread_create(&ctx->threads[i], NULL, worker_entry, arg);
    }

    return ctx;
}

void destroyParallelBatchContext(ParallelBatchContext *ctx) {
    // No safe shutdown here yet — threads run infinitely.
    // TODO: Add shutdown flag and join logic if needed.

    for (int i = 0; i < ctx->num_threads; ++i) {
        for (int j = 0; j < NUM_BUFFERS; ++j) {
            free(ctx->worker_buffers[i].buffers[j]);
        }
        pthread_mutex_destroy(&ctx->worker_buffers[i].mutex);
        pthread_cond_destroy(&ctx->worker_buffers[i].cond);
    }

    free(ctx->threads);
    free(ctx->thread_args);
    free(ctx->worker_buffers);

    pthread_mutex_destroy(&ctx->start_mutex);
    pthread_cond_destroy(&ctx->start_cond);
    free(ctx);
}

void runParallelBatch(ParallelBatchContext *ctx, void *user_data) {
    // Assign work
    for (int i = 0; i < ctx->num_threads; ++i) {
        ThreadArgs *arg = &ctx->thread_args[i];
        arg->user_data = user_data;
    }

    // Signal all threads to begin
    atomic_store(&ctx->start_flag, true);
    pthread_mutex_lock(&ctx->start_mutex);
    pthread_cond_broadcast(&ctx->start_cond);
    pthread_mutex_unlock(&ctx->start_mutex);

    // Wait for all threads to mark themselves done
    while (1) {
        int all_done = 1;
        for (int i = 0; i < ctx->num_threads; ++i) {
            if (!atomic_load(&ctx->worker_buffers[i].is_done)) {
                all_done = 0;
                break;
            }
        }
        if (all_done) break;
        usleep(1000);
    }

    // Reset flag for next batch
    atomic_store(&ctx->start_flag, false);
}
