#include "rdb_mt.h"
#include "rdb.h"
#include <errno.h>
#include <string.h>
#include <unistd.h>

static void *process_slots_with_iterator(void *arg);

ustime_t runSaveThreads(rio *rdb, int dbid, int num_threads, char *pname) {
    const long long start = ustime();

    _Atomic long shared_keys_processed = ATOMIC_VAR_INIT(0);
    _Atomic long shared_last_info_time_ms = ATOMIC_VAR_INIT(0);

    pthread_mutex_t shared_write_mutex;
    if (pthread_mutex_init(&shared_write_mutex, NULL) != 0) {
        serverLog(LL_WARNING, "Failed to initialize shared mutex: %s", strerror(errno));
    }

    pthread_t threads[num_threads];
    ThreadArgs thread_args[num_threads];

    for (int i = 0; i < num_threads; ++i) {
        thread_args[i] = (ThreadArgs){
            .thread_id = i,
            .num_threads = num_threads,
            .dbid = dbid,
            .write_mutex = &shared_write_mutex,
            .rdb = rdb,
            .shared_keys_processed = &shared_keys_processed,
            .shared_last_info_time_ms = &shared_last_info_time_ms,
            .pname = pname
        };

        if (pthread_create(&threads[i], NULL, process_slots_with_iterator, &thread_args[i]) != 0) {
            serverLog(LL_WARNING, "Failed to create thread %d: %s", i, strerror(errno));
            for (int j = 0; j < i; ++j) {
                pthread_cancel(threads[j]);
                pthread_join(threads[j], NULL);
            }
            pthread_mutex_destroy(&shared_write_mutex);
            return 0;
        }
    }

    for (int i = 0; i < num_threads; ++i) {
        pthread_join(threads[i], NULL);
    }

    pthread_mutex_destroy(&shared_write_mutex);

    serverLog(LL_NOTICE, "Total keys processed across all threads: %ld",
              atomic_load(&shared_keys_processed));

    return ustime() - start;
}


static void *process_slots_with_iterator(void *arg) {
    ThreadArgs *args = arg;

    int thread_id = args->thread_id;
    int dbid = args->dbid;
    rio *rdb = args->rdb;
    pthread_mutex_t *write_mutex = args->write_mutex;
    _Atomic long *shared_keys_processed = args->shared_keys_processed;
    _Atomic long *shared_last_info_time_ms = args->shared_last_info_time_ms;
    char *pname = args->pname;

    serverDb *db = server.db + dbid;
    kvstoreIterator *kvs_it = kvstoreIteratorInit(db->keys, HASHTABLE_ITER_SAFE | HASHTABLE_ITER_PREFETCH_VALUES);
    if (!kvs_it) {
        serverLog(LL_WARNING, "Thread %d: Failed to init kvstore iterator: %s", thread_id, strerror(errno));
        return NULL;
    }

    void *next;
    int last_slot = -1, current_thread_keys = 0;
    ssize_t res, written_to_rio = 0;

    while (kvstoreIteratorNext(kvs_it, &next)) {
        int curr_slot = kvstoreIteratorGetCurrentHashtableIndex(kvs_it);
        if ((curr_slot % args->num_threads) != args->thread_id) continue;

        pthread_mutex_lock(write_mutex);

        if (server.cluster_enabled && curr_slot != last_slot) {
            sds slot_info = sdscatprintf(sdsempty(), "%i,%lu,%lu", curr_slot,
                kvstoreHashtableSize(db->keys, curr_slot),
                kvstoreHashtableSize(db->expires, curr_slot));
            if ((res = rdbSaveAuxFieldStrStr(rdb, "slot-info", slot_info)) < 0) {
                sdsfree(slot_info);
                pthread_mutex_unlock(write_mutex);
                goto werr;
            }
            written_to_rio += res;
            last_slot = curr_slot;
            sdsfree(slot_info);
        }

        robj *o = next;
        sds keystr = objectGetKey(o);
        robj key;
        long long expire;
        size_t rdb_bytes_before_key = rdb->processed_bytes;

        initStaticStringObject(key, keystr);
        expire = getExpire(db, &key);

        if ((res = rdbSaveKeyValuePair(rdb, &key, o, expire, dbid)) < 0) {
            pthread_mutex_unlock(write_mutex);
            goto werr;
        }
        written_to_rio += res;

        if (server.in_fork_child) {
            size_t dump_size = rdb->processed_bytes - rdb_bytes_before_key;
            dismissObject(o, dump_size);
        }

        pthread_mutex_unlock(write_mutex);

        long current_total_keys = atomic_fetch_add(shared_keys_processed, 1);
        current_thread_keys++;

        if (((current_total_keys + 1) & 1023) == 0) {
            long long now = mstime();
            long long old = atomic_load(shared_last_info_time_ms);
            if (now - old >= 1000) {
                if (atomic_compare_exchange_strong(shared_last_info_time_ms, &old, now)) {
                    sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, current_total_keys + 1, pname);
                }
            }
        }

        serverLog(LL_NOTICE, "Iterator Thread %d processing key: %s (slot %d)",
                  thread_id, keystr, curr_slot);
    }

    kvstoreIteratorRelease(kvs_it);

    pthread_mutex_lock(write_mutex);
    serverLog(LL_NOTICE, "Iterator Thread %d processed %d keys.", thread_id, current_thread_keys);
    pthread_mutex_unlock(write_mutex);
    return NULL;

werr:
    if (kvs_it) kvstoreIteratorRelease(kvs_it);
    serverLog(LL_WARNING, "Thread %d RDB save error.", thread_id);
    pthread_mutex_unlock(write_mutex);
    return NULL;
}
