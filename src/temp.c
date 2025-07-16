// /* IO jobs queue functions - Used to send jobs from the main-thread to the IO thread. */
// typedef void (*job_handler)(void *);
// typedef struct iojob {
//     job_handler handler;
//     void *data;
// } iojob;

// typedef struct IOJobQueue {
//     iojob *ring_buffer;
//     size_t size;
//     _Atomic size_t head __attribute__((aligned(CACHE_LINE_SIZE))); /* Next write index for producer (main-thread) */
//     _Atomic size_t tail __attribute__((aligned(CACHE_LINE_SIZE))); /* Next read index for consumer  (IO-thread) */
// } IOJobQueue;

// /* Initialize the job queue with a specified number of items. */
// static void IOJobQueue_init(IOJobQueue *jq, size_t item_count) {
//     debugServerAssertWithInfo(NULL, NULL, inMainThread());
//     jq->ring_buffer = zcalloc(item_count * sizeof(iojob));
//     jq->size = item_count; /* Total number of items */
//     jq->head = 0;
//     jq->tail = 0;
// }

// /* Clean up the job queue and free allocated memory. */
// static void IOJobQueue_cleanup(IOJobQueue *jq) {
//     debugServerAssertWithInfo(NULL, NULL, inMainThread());
//     zfree(jq->ring_buffer);
//     memset(jq, 0, sizeof(*jq));
// }

// static int IOJobQueue_isFull(const IOJobQueue *jq) {
//     debugServerAssertWithInfo(NULL, NULL, inMainThread());
//     size_t current_head = atomic_load_explicit(&jq->head, memory_order_relaxed);
//     /* We don't use memory_order_acquire for the tail due to performance reasons,
//      * In the worst case we will just assume wrongly the buffer is full and the main thread will do the job by itself. */
//     size_t current_tail = atomic_load_explicit(&jq->tail, memory_order_relaxed);
//     size_t next_head = (current_head + 1) % jq->size;
//     return next_head == current_tail;
// }

// /* Attempt to push a new job to the queue from the main thread.
//  * the caller must ensure the queue is not full before calling this function. */
// static void IOJobQueue_push(IOJobQueue *jq, job_handler handler, void *data) {
//     debugServerAssertWithInfo(NULL, NULL, inMainThread());
//     /* Assert the queue is not full - should not happen as the caller should check for it before. */
//     serverAssert(!IOJobQueue_isFull(jq));

//     /* No need to use atomic acquire for the head, as the main thread is the only one that writes to the head index. */
//     size_t current_head = atomic_load_explicit(&jq->head, memory_order_relaxed);
//     size_t next_head = (current_head + 1) % jq->size;

//     /* We store directly the job's fields to avoid allocating a new iojob structure. */
//     serverAssert(jq->ring_buffer[current_head].data == NULL);
//     serverAssert(jq->ring_buffer[current_head].handler == NULL);
//     jq->ring_buffer[current_head].data = data;
//     jq->ring_buffer[current_head].handler = handler;

//     /* memory_order_release to make sure the data is visible to the consumer (the IO thread). */
//     atomic_store_explicit(&jq->head, next_head, memory_order_release);
// }

// /* Returns the number of jobs currently available for consumption in the given job queue.
//  *
//  * This function  ensures memory visibility for the jobs by
//  * using a memory acquire fence when there are jobs available. */
// static size_t IOJobQueue_availableJobs(const IOJobQueue *jq) {
//     debugServerAssertWithInfo(NULL, NULL, !inMainThread());
//     /* We use memory_order_acquire to make sure the head and the job's fields are visible to the consumer (IO thread). */
//     size_t current_head = atomic_load_explicit(&jq->head, memory_order_acquire);
//     size_t current_tail = atomic_load_explicit(&jq->tail, memory_order_relaxed);

//     if (current_head >= current_tail) {
//         return current_head - current_tail;
//     } else {
//         return jq->size - (current_tail - current_head);
//     }
// }

// /* Checks if the job Queue is empty.
//  * returns 1 if the buffer is currently empty, 0 otherwise.
//  * Called by the main-thread only.
//  * This function uses relaxed memory order, so the caller need to use an acquire
//  * memory fence before calling this function to be sure it has the latest index
//  * from the other thread, especially when called repeatedly. */
// static int IOJobQueue_isEmpty(const IOJobQueue *jq) {
//     size_t current_head = atomic_load_explicit(&jq->head, memory_order_relaxed);
//     size_t current_tail = atomic_load_explicit(&jq->tail, memory_order_relaxed);
//     return current_head == current_tail;
// }

// /* Removes the next job from the given job queue by advancing the tail index.
//  * Called by the IO thread.
//  * The caller must ensure that the queue is not empty before calling this function.
//  * This function uses relaxed memory order, so the caller need to use an release memory fence
//  * after calling this function to make sure the updated tail is visible to the producer (main thread). */
// static void IOJobQueue_removeJob(IOJobQueue *jq) {
//     debugServerAssertWithInfo(NULL, NULL, !inMainThread());
//     size_t current_tail = atomic_load_explicit(&jq->tail, memory_order_relaxed);
//     jq->ring_buffer[current_tail].data = NULL;
//     jq->ring_buffer[current_tail].handler = NULL;
//     atomic_store_explicit(&jq->tail, (current_tail + 1) % jq->size, memory_order_relaxed);
// }

// /* Retrieves the next job handler and data from the job queue without removal.
//  * Called by the consumer (IO thread). Caller must ensure queue is not empty.*/
// static void IOJobQueue_peek(const IOJobQueue *jq, job_handler *handler, void **data) {
//     debugServerAssertWithInfo(NULL, NULL, !inMainThread());
//     size_t current_tail = atomic_load_explicit(&jq->tail, memory_order_relaxed);
//     iojob *job = &jq->ring_buffer[current_tail];
//     *handler = job->handler;
//     *data = job->data;
// }

// /* End of IO job queue functions */

// int inMainThread(void) {
//     return thread_id == 0;
// }

// int getIOThreadID(void) {
//     return thread_id;
// }