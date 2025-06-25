#ifndef __THREAD_POOL_H__
#define __THREAD_POOL_H__

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include "concurrent_queue.h"

typedef struct task {
    void (*task_func)(void *); // Function the thread should run
    void *arg;                 // Argument for the task function
} task;

typedef struct threadPool {
    pthread_t *threads;
    int num_threads;
    threadSafeQueue *task_queue;
    pthread_mutex_t pool_state_mutex;
    pthread_cond_t all_tasks_done_cond;
    int active_tasks;
    bool shutting_down;
} threadPool;

/** Create and initialize a new thread pool.
 * Returns pointer to the threadPool on success, NULL on failure
 */
threadPool *threadPoolCreate(int num_threads);

/** Add a task to the thead pool's queue
 * 'func' is the function to execute, arg is argument for this function
 * Returns 1 on success, 0 on failure
 */
int threadPoolAddTask(threadPool *pool, void (*func)(void *), void *arg);


/** Wait for all currently added tasks to complete.
 * This function BLOCKS until 'active_tasks' is 0
 * Note: This does not destroy the pool.
 */
void threadPoolWaitAll(threadPool *pool);

/* Non-blocking check to see if all currently added tasks are done.
 * Returns true if all tasks are complete (active_task is 0), false otherwise. */
bool threadPoolAreAllTasksDone(threadPool *pool);


/** Gracefully shutdown the thread pool.
 * This signals the pool to stop accepting new tasks, waits for all
 * current tasks to complete, then joins all worker threads.
 */
void threadPoolDestroy(threadPool *pool);


#endif // __THREAD_POOL_H__
