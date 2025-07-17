#ifndef __RDB_THREADS_H__
#define __RDB_THREADS_H__

#include "server.h"
#include "thread_common.h"

void initRDBThreads(int per_thread_queue_size);
void killRDBThreads(void);


#endif // __RDB_THREADS_H__