#include "concurrent_queue.h"
#include "zmalloc.h"
#include <stdio.h>
#include "adlist.h"
#include <pthread.h>

static void listFreeValueAndNode(void *val) {
    zfree(val); // free the task struct
}

void concurrentQueueInit(threadSafeQueue *q) {
    q->tasks = listCreate();

    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond, NULL);
    q->shutdown = false;
}

void concurrentQueuePush(threadSafeQueue *q, void *task) {
    pthread_mutex_lock(&q->mutex);
    listAddNodeTail(q->tasks, task);
    pthread_cond_signal(&q->cond); // Send signal to one worker that a task is available
    pthread_mutex_unlock(&q->mutex);
}

/* Pop a task from the queue. Returns NULL if the queue is empty
 * and shutdown has been signaled.
 * The caller (consumer) is responsible for freeing the returned task_ptr. */
void *concurrentQueuePop(threadSafeQueue *q) {
    void *task_ptr = NULL;

    pthread_mutex_lock(&q->mutex);

    // Wait while queue is empty AND not shutting down
    while (listLength(q->tasks) == 0 && !q->shutdown) {
        pthread_cond_wait(&q->cond, &q->mutex);
    }

    // If shutting down and queue is empty return NULL
    if (q->shutdown && listLength(q->tasks) == 0) {
        pthread_mutex_unlock(&q->mutex);
        return NULL;
    }

    listNode *node = listFirst(q->tasks);
    if (node) {
        task_ptr = node->value;
        // NOTE: listDelNode does not free the memory pointed to by node->value.
        // it is the consumer of this tasks responsibility to free this memory.
        listDelNode(q->tasks, node);
    }
    pthread_mutex_unlock(&q->mutex);
    return task_ptr;
}

void concurrentQueueShutdown(threadSafeQueue *q) {
    pthread_mutex_lock(&q->mutex);
    q->shutdown = true;
    pthread_cond_broadcast(&q->cond); // Wake up all waiting workers
    pthread_mutex_unlock(&q->mutex);
}

/* Destroy the queue by freeing the list.
 * Only call this after all threads have finished .
 * WARNING: This will NOT free any remaining 'task' structs in the queue.
 * The user should make sure the queue is empty of tasks or manually free them
 * before calling this
 */
void concurrentQueueRelease(threadSafeQueue *q) {
    listRelease(q->tasks);
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond);
}
