// rdb_parallel_batch.h
#pragma once

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>

#define NUM_BUFFERS 2

#define BUFFER_FREE 0
#define BUFFER_READY 1

typedef struct {
    char *buffers[NUM_BUFFERS];
    atomic_int status[NUM_BUFFERS];
    int current_write_index;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    atomic_bool is_done;
} WorkerBuffer;

typedef struct {
    int thread_id;
    int num_threads;
    pthread_cond_t *start_cond;
    pthread_mutex_t *start_mutex;
    atomic_bool *start_flag;
    WorkerBuffer *worker_buffer;
    void (*work_fn)(void *ctx); // Per-thread work function
    void *user_data;            // Pointer to the current hashtable
} ThreadArgs;

typedef struct {
    pthread_t *threads;
    ThreadArgs *thread_args;
    WorkerBuffer *worker_buffers;
    int num_threads;
    pthread_cond_t start_cond;
    pthread_mutex_t start_mutex;
    atomic_bool start_flag;

    void (*work_fn)(void *ctx);
} ParallelBatchContext;

ParallelBatchContext *createParallelBatchContext(int num_threads, size_t buffer_size, void (*work_fn)(void *));
void destroyParallelBatchContext(ParallelBatchContext *ctx);

// Run a parallel batch over a new "user_data" context (e.g., a hashtable)
void runParallelBatch(ParallelBatchContext *ctx, void *user_data);
