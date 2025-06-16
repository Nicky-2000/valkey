#!/bin/bash

# Ensure valkey-server instances on 7000 and 7001 are running and clean before running this script.
src/valkey-server ./testconfs/valkey7000.conf
sleep 1 
src/valkey-server ./testconfs/valkey7001.conf 
sleep 2
printf "yes\nyes\n" | src/valkey-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 --cluster-replicas 0
sleep 3


NUM_HASHS_PER_LETTER=1
NUM_KEYS_PER_HASH=1
EXPECTED_TOTAL_KEYS=$(( NUM_HASHS_PER_LETTER * NUM_KEYS_PER_HASH * 26 ))

echo "--- Populating ValKey Cluster ---"
for index in $(seq 1 $NUM_HASHS_PER_LETTER); do
    for letter in {A..Z}; do
        for i in $(seq 1 $NUM_KEYS_PER_HASH); do
            KEY="{hash${letter}${index}}key_$i"
            VALUE="value_${letter}_${index}_$i"
            src/valkey-cli -c -p 7000 set "${KEY}" "${VALUE}" > /dev/null
        done
    done
done
echo "--- Data population complete. ---"

# Verify initial key count
# echo "Verifying initial key count..."
# sleep 1 # Give time for replication/cluster sync
# CURRENT_KEYS=$(src/valkey-cli -c -p 7000 DBSIZE)
# if [ "$CURRENT_KEYS" -eq "$EXPECTED_TOTAL_KEYS" ]; then
#     echo "Initial keys (total): $CURRENT_KEYS (OK)"
# else
#     echo "Initial keys (total): $CURRENT_KEYS (MISMATCH! Expected $EXPECTED_TOTAL_KEYS)"
#     exit 1
# fi

echo "--- Triggering RDB Save (BGSAVE) ---"
src/valkey-cli -p 7000 BGSAVE > /dev/null # Trigger BGSAVE on one primary
sleep 2
src/valkey-cli -p 7001 BGSAVE > /dev/null # Trigger BGSAVE on the other primary
sleep 2 # Give BGSAVE time to complete. Adjust if your data is much larger.

echo "--- Flushing all keys ---"
src/valkey-cli -p 7000 FLUSHALL > /dev/null
src/valkey-cli -p 7001 FLUSHALL > /dev/null
sleep 1 # Give time for flush to propagate
CURRENT_KEYS=$(src/valkey-cli -c -p 7000 DBSIZE)
if [ "$CURRENT_KEYS" -eq 0 ]; then
    echo "Keys after FLUSHALL: $CURRENT_KEYS (OK)"
else
    echo "Keys after FLUSHALL: $CURRENT_KEYS (FAIL! Expected 0)"
    exit 1
fi

echo "--- Stopping ValKey Servers ---"
src/valkey-cli -p 7000 shutdown nosave > /dev/null # Shutdown without saving again
src/valkey-cli -p 7001 shutdown nosave > /dev/null
sleep 2 # Give servers time to stop

echo "--- Restarting ValKey Servers to Load RDB ---"
# Assuming your valkey-server commands are something like this, adjust if needed
nohup src/valkey-server ./testconfs/valkey7000.conf > /dev/null 2>&1 &
nohup src/valkey-server ./testconfs/valkey7001.conf > /dev/null 2>&1 &
sleep 5 # Give servers time to start up and load RDB

echo "--- Verifying keys after RDB load ---"
CURRENT_KEYS=$(src/valkey-cli -c -p 7000 DBSIZE)
if [ "$CURRENT_KEYS" -eq "$EXPECTED_TOTAL_KEYS" ]; then
    echo "RDB Load Test: SUCCESS! Keys after load: $CURRENT_KEYS (Expected $EXPECTED_TOTAL_KEYS)"
else
    echo "RDB Load Test: FAILURE! Keys after load: $CURRENT_KEYS (Expected $EXPECTED_TOTAL_KEYS)"
fi

echo "--- Test Complete ---"

# Optional: Clean up servers (stops and cleans data dirs)
echo "--- Cleaning up servers ---"
src/valkey-cli -p 7000 shutdown > /dev/null 2>&1
src/valkey-cli -p 7001 shutdown > /dev/null 2>&1
rm -rf ${HOME}/Documents/valkey/tmp/valkey-cluster-7000/*
rm -rf ${HOME}/Documents/valkey/tmp/valkey-cluster-7001/*