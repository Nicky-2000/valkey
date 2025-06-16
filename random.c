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
