#include "thread_pool.h"
#include "server.h"
#include "serverassert.h"
#include "zmalloc.h"
#include <stdio.h>
#include <stdlib.h>

/* Entrypoint for a worker thread.
 * Continuously pulls and executes tasks from the queue
 */
static void *workerThreadEntryPoint(void *arg) {
    threadPool *pool = (threadPool *)arg;
    concurrentQueue *task_queue = pool->task_queue;
    task *t;

    while (1) {
        // This will block until a task is recieved.
        t = (task *)concurrentQueuePop(task_queue);

        if (t == NULL) {
            // The queue is empty AND the queue is shutting down
            break;
        }

        // Execute the task
        t->task_func(t->arg);

        pthread_mutex_lock(&pool->pool_state_mutex);
        pool->active_tasks--;

        if (pool->active_tasks == 0) {
            pthread_cond_signal(&pool->all_tasks_done_cond);
        }
        pthread_mutex_unlock(&pool->pool_state_mutex);

        zfree(t);
    }
    return NULL;
}

threadPool *threadPoolCreate(int num_threads) {
    serverAssert(num_threads > 0);

    threadPool *pool = zmalloc(sizeof(threadPool));
    if (!pool) {
        return NULL;
    }

    pool->num_threads = num_threads;
    pool->shutting_down = false;
    pool->active_tasks = 0;

    // Allocate and initialize task queue
    pool->task_queue = zmalloc(sizeof(concurrentQueue));
    if (!pool->task_queue) {
        zfree(pool);
        return NULL;
    }
    concurrentQueueInit(pool->task_queue);

    // Allocate space for threads
    pool->threads = zmalloc(sizeof(pthread_t) * num_threads);
    if (!pool->threads) {
        concurrentQueueRelease(pool->task_queue);
        zfree(pool->task_queue);
        zfree(pool);
        return NULL;
    }

    pthread_mutex_init(&pool->pool_state_mutex, NULL);
    pthread_cond_init(&pool->all_tasks_done_cond, NULL);

    // Initialize threads
    for (int i = 0; i < num_threads; i++) {
        if (pthread_create(&pool->threads[i], NULL, workerThreadEntryPoint, pool) != 0) {
            serverLog(LL_WARNING, "Unable to create threads for threadPool");
            
            // Shut down queue so threads will not be blocked waiting to aquire a task
            concurrentQueueShutdown(pool->task_queue);


            for (int j = 0; j < i; ++j) {
                pthread_join(pool->threads[j], NULL);
            }
            
            // Free all the allocated resources
            pthread_mutex_destroy(&pool->pool_state_mutex);
            pthread_cond_destroy(&pool->all_tasks_done_cond);

            concurrentQueueRelease(pool->task_queue);
            zfree(pool->task_queue);
            zfree(pool->threads);
            zfree(pool);
            return NULL;
        }
    }
    serverLog(LL_NOTICE, "threadPoolCreate - Success");
    return pool;
}


int threadPoolAddTask(threadPool *pool, void (*func)(void *), void *arg) {
    pthread_mutex_lock(&pool->pool_state_mutex);
    if (pool->shutting_down) {
        serverLog(LL_DEBUG, "threadPoolAddTask - Failed. Pool is shutting down");
        pthread_mutex_unlock(&pool->pool_state_mutex);
        return 0;
    }
    pool->active_tasks++;
    pthread_mutex_unlock(&pool->pool_state_mutex);

    task *new_task = zmalloc(sizeof(task));
    if (!new_task) {
        pthread_mutex_lock(&pool->pool_state_mutex);
        pool->active_tasks--;
        pthread_mutex_unlock(&pool->pool_state_mutex);
        return 0;
    }
    new_task->task_func = func;
    new_task->arg = arg;
    concurrentQueuePush(pool->task_queue, new_task);
    return 1;
}

void threadPoolWaitAll(threadPool *pool) {
    pthread_mutex_lock(&pool->pool_state_mutex);
    while(pool->active_tasks > 0) {
        pthread_cond_wait(&pool->all_tasks_done_cond, &pool->pool_state_mutex);
    }
    pthread_mutex_unlock(&pool->pool_state_mutex);
}

bool threadPoolAreAllTasksDone(threadPool *pool) {
    pthread_mutex_lock(&pool->pool_state_mutex);
    bool done = (pool->active_tasks == 0);
    pthread_mutex_unlock(&pool->pool_state_mutex);
    return done;
}

void threadPoolDestroy(threadPool *pool) {
    if (!pool) return;

    // Step 1: Signal pool shutdown to stop future threadPoolAddTask calls
    pthread_mutex_lock(&pool->pool_state_mutex);
    pool->shutting_down = true; 
    pthread_mutex_unlock(&pool->pool_state_mutex);

    // Step 2: Signal the queue to shut down, waking all workers
    // This is crucial to unblock any workers waiting for tasks
    concurrentQueueShutdown(pool->task_queue);

    // Step 3: Wait for any remaining tasks to finish
    threadPoolWaitAll(pool);

    // Step 4: Join all worker threads
    for (int i = 0; i < pool->num_threads; i++) {
        if (pthread_join(pool->threads[i], NULL) != 0) {
            serverLog(LL_WARNING, "Failed to join thread");
        }
    }

    // Step 5: Clean up all the allocated resources
    pthread_mutex_destroy(&pool->pool_state_mutex);
    pthread_cond_destroy(&pool->all_tasks_done_cond);

    concurrentQueueRelease(pool->task_queue);
    zfree(pool->task_queue);
    zfree(pool->threads);
    zfree(pool);
}
