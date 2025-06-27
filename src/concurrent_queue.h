#ifndef __CONCURRENT_QUEUE_H__
#define __CONCURRENT_QUEUE_H__

#include "adlist.h"
#include <pthread.h>
#include <stdbool.h>

typedef struct task task;

/* Thread safe queue using Valkey's adlist */
typedef struct concurrentQueue {
    list *tasks;           // The Valkey list to hold 'task' structs (or any void*)
    pthread_mutex_t mutex; // Protects access to the 'tasks' list and shutdown flag
    pthread_cond_t cond;   // Signal when tasks are available on the queue or state changes
    bool shutdown;         // Flag to signal workers to exit... do we need this?

} concurrentQueue;

void concurrentQueueInit(concurrentQueue *q);
void concurrentQueuePush(concurrentQueue *q, void *task);
void *concurrentQueuePop(concurrentQueue *q);
void concurrentQueueShutdown(concurrentQueue *q);
void concurrentQueueRelease(concurrentQueue *q);

#endif // __CONCURRENT_QUEUE_H__
