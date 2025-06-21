// void *process_slot_threads_bitmap(void* arg) {
//     ThreadArgs *args = (ThreadArgs*) arg;
//     int thread_id = args->thread_id;
//     int num_threads = args-> num_threads;
//     clusterNode *myself = args->myself;

//     int slots_processed_by_thread = 0; 
//     for (int byte_idx = 0; byte_idx < (CLUSTER_SLOTS / 8); byte_idx++) {
//         unsigned char current_byte = myself->slots[byte_idx]; // one array lookup    
        
//         for (int bit_pos = 0; bit_pos < 8; bit_pos++) {
//             if ((current_byte >> bit_pos) & 1) {
//                 int slot_index = (byte_idx * 8) + bit_pos;

//                 if ((slot_index % num_threads) == thread_id) {
//                     // Here we will process and save the data for this slot.
//                     // serverLog(LL_NOTICE, "Thread %d in Node %.40s: Processing slot %d\n", thread_id, myself->name, i);
//                     int x = 10; 
//                     int y = x - 10; 
//                     if (y != 100) {
//                         serverLog(LL_VERBOSE ,"DOING WORK");
//                     }
//                     slots_processed_by_thread++;
//                 }
//             }
//         }
//     }
//     serverLog(LL_NOTICE, "Thread %d processed %d slots:", thread_id, slots_processed_by_thread);
//     return NULL;
// }


// void *process_slots_thread_ptr_lookup(void *arg) {
//     ThreadArgs *args = (ThreadArgs *)arg;
//     int thread_id = args->thread_id;
//     int num_threads = args->num_threads;
//     int dbid = args->dbid; 
//     int slots_processed_by_thread = 0;

//     // Iterate through all possible slots
//     for (int i = 0; i <= CLUSTER_SLOTS; i++) {
//         if (myself != getNodeBySlot(i)) continue;
        
//         // (current_slot % total_threads) == this_thread_id
//         if ((i % num_threads) == thread_id ) {

//             // This slot belongs to node and this node and this thread.
//             // serverLog(LL_NOTICE, "Thread %d in Node %.40s: Processing slot %d\n", thread_id, myself->name, i);

//             int x = 10; 
//             int y = x - 10; 
//             if (y != 100) {
//                 serverLog(LL_VERBOSE ,"DOING WORK");
//             }
//             slots_processed_by_thread++;

//             // Simulate some work being done per slot (optional)
//             // usleep(10); // Sleep for 10 microseconds
//         }
//     }
//     serverLog(LL_NOTICE, "Thread %d processed %d slots:", thread_id, slots_processed_by_thread);
//     return NULL;
// }


// void *process_slots_with_iterator(void *arg) {
//     ThreadArgs *args = (ThreadArgs *)arg;

//     int thread_id = args->thread_id;
//     int num_threads = args->num_threads;
//     int dbid = args->dbid;
//     rio *rdb = args->rdb; // The shared RIO object
//     pthread_mutex_t *write_mutex = args->write_mutex; // The shared mutex
//     _Atomic long *shared_keys_processed = args->shared_keys_processed; // Pointer to shared atomic keys counter
//     _Atomic long *shared_last_info_time_ms = args->shared_last_info_time_ms; // Pointer to shared atomic timestamp
//     char *pname = args->pname; // The process name string (e.g., "RDB")
    
//     int start_ht_idx = args->start_ht_idx;
//     int end_ht_idx = args->end_ht_idx;

//     int current_thread_keys = 0;
//     int last_slot = -1;
//     ssize_t written_to_rio = 0;
//     ssize_t res;

//     serverDb *db = server.db + dbid;

//     void *next;
//     kvstoreIterator *kvs_it = kvstoreIteratorInitFromIndex(db->keys, HASHTABLE_ITER_SAFE | HASHTABLE_ITER_PREFETCH_VALUES, start_ht_idx);

//     if (!kvs_it) {
//         serverLog(LL_WARNING, "Thread %d: Failed to initialize kvstore iterator. Error: %s",
//                    thread_id, strerror(errno));
//         return NULL;
//     }

//     // Use kvstoreIteratorNextWithEnd to iterate only within the assigned range
//     while (kvstoreIteratorNextWithEnd(kvs_it, &next, end_ht_idx)) {
//         int curr_slot = kvstoreIteratorGetCurrentHashtableIndex(kvs_it);        
        
//         /* Critical Section Starts: Protect RIO writes with Mutex.         
//         /* Save slot info. */
//         if (server.cluster_enabled && curr_slot != last_slot) {
//             pthread_mutex_lock(args->write_mutex);

//             sds slot_info = sdscatprintf(sdsempty(), "%i,%lu,%lu", curr_slot, kvstoreHashtableSize(db->keys, curr_slot),
//                                          kvstoreHashtableSize(db->expires, curr_slot));
//             if ((res = rdbSaveAuxFieldStrStr(rdb, "slot-info", slot_info)) < 0) {
//                 sdsfree(slot_info);
//                 pthread_mutex_unlock(write_mutex); // Unlock before jumping to error handler
//                 goto werr;
//             }
//             pthread_mutex_unlock(write_mutex);
//             written_to_rio += res;
//             last_slot = curr_slot;
//             sdsfree(slot_info);
//         }
        
//         /* Save key-value pair. */
//         robj *o = next;
//         sds keystr = objectGetKey(o);
//         robj key;
//         long long expire;
//         size_t rdb_bytes_before_key = rdb->processed_bytes;

//         initStaticStringObject(key, keystr);
//         expire = getExpire(db, &key); 

//         pthread_mutex_lock(args->write_mutex);
//         if ((res = rdbSaveKeyValuePair(rdb, &key, o, expire, dbid)) < 0) {
//             pthread_mutex_unlock(write_mutex);
//             goto werr;
//         }

//         pthread_mutex_unlock(write_mutex); 
//         written_to_rio += res;

//         /* In fork child process, we can try to release memory back to the
//          * OS and possibly avoid or decrease COW. We give the dismiss
//          * mechanism a hint about an estimated size of the object we stored. */
//         size_t dump_size = rdb->processed_bytes - rdb_bytes_before_key;
//         if (server.in_fork_child) dismissObject(o, dump_size);

//         /* Critical Section Over */

//         long current_total_keys_before_add = atomic_fetch_add(shared_keys_processed, 1);
//         current_thread_keys++;



//         // Send update to the parent process. 
//         // The simplest way to update the parent process is to use one thread.
//         // However, this is a single point of failure. What if the thread gets starved?
//         // Very unlikely that a thread would be starved for more than a second though.

//         // Logic to have each thread be part of the update is very complex and relies on
//         // more shared variables. 
//         // This block reports total progress to the main server process.
//         // It's checked every 1024 keys *globally* and every ~1 second.
//         if (thread_id == 0 && ((current_total_keys_before_add + 1) & 1023) == 0) {
//             long long now = mstime();
//             long long old_info_time_val = atomic_load(shared_last_info_time_ms); // Atomic read of the last time this was set

//             if (now - old_info_time_val >= 1000) {
//                 if (atomic_compare_exchange_strong(shared_last_info_time_ms, &old_info_time_val, now)){
//                     sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, current_total_keys_before_add + 1, pname);
//                 }
//             }
//         }
//     }
//     kvstoreIteratorRelease(kvs_it);
//     // Final logs (should also be mutex-protected if serverLog is truly not thread-safe)
//     pthread_mutex_lock(write_mutex);
//     serverLog(LL_NOTICE, "Iterator Thread %d processed %d keys (local count).", thread_id, current_thread_keys);
//     pthread_mutex_unlock(write_mutex);

//     return NULL;

// werr: // Error handling for RIO writes
//     if (kvs_it) kvstoreIteratorRelease(kvs_it);
//     // Log error, and ensure mutex is unlocked even on error path
//     serverLog(LL_WARNING, "Thread %d RDB save error. Unlocking mutex if locked.", thread_id);
//     pthread_mutex_unlock(write_mutex); // Ensure unlock on error
//     return NULL;
// }
