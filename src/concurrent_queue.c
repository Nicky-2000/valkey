#include <stdio.h>
#include <pthread.h>
#include "adlist.h"
#include "concurrent_queue.h"
#include "zmalloc.h"


void concurrentQueueInit(concurrentQueue *q) {
    q->tasks = listCreate();

    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond, NULL);
    q->shutdown = false;
}

void concurrentQueuePush(concurrentQueue *q, void *task) {
    pthread_mutex_lock(&q->mutex);
    listAddNodeTail(q->tasks, task);
    pthread_cond_signal(&q->cond); // Send signal to one worker that a task is available
    pthread_mutex_unlock(&q->mutex);
}

/* Pop a task from the queue. Returns NULL if the queue is empty
 * and shutdown has been signaled.
 * The caller (consumer) is responsible for freeing the returned task_ptr. */
void *concurrentQueuePop(concurrentQueue *q) {
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

void concurrentQueueShutdown(concurrentQueue *q) {
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
void concurrentQueueRelease(concurrentQueue *q) {
    listRelease(q->tasks);
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond);
}
