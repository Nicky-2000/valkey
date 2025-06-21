#!/bin/bash

# Step 1: Clear all the directories for the ValKey servers
rm -rf tmp/valkey-cluster-7000/*
rm -rf tmp/valkey-cluster-7001/*

# Step 2: Start the two servers
# Store their PIDs to kill them later.
echo "Starting Valkey server on port 7000..."
./src/valkey-server testconfs/valkey7000.conf &
PID_7000=$!
echo "Valkey server 7000 PID: $PID_7000"

echo "Starting Valkey server on port 7001..."
./src/valkey-server testconfs/valkey7001.conf  &
PID_7001=$!
echo "Valkey server 7001 PID: $PID_7001"

# Give servers a moment to start up
sleep 2

# Step 3: Connect the servers in cluster mode
printf "yes\nyes\n" | src/valkey-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 --cluster-replicas 0
sleep 2 # Give the cluster some time to stabilize

# Step 4: Populate the servers with some keys
NUM_HASHS_PER_LETTER=100
NUM_KEYS_PER_HASH=10
echo "--- Populating ValKey Cluster ---"
echo "Total expected keys: $(( ${NUM_HASHS_PER_LETTER} * ${NUM_KEYS_PER_HASH} * 26 ))"

KEY_COUNTER=0

# Loop through each letter of the alphabet (A to Z)
for index in $(seq 1 $NUM_HASHS_PER_LETTER); do
    for letter in {A..Z}; do
        # echo "--- Adding {hash${letter}${index}} keys ---"
        for i in $(seq 1 $NUM_KEYS_PER_HASH); do
            KEY="{hash${letter}${index}}key_$i"
            VALUE="value_${letter}_${index}_$i"
            
            # Send the SET command using the specified format
            src/valkey-cli -c -p 7000 set "${KEY}" "${VALUE}" > /dev/null 2>&1

            # Increment the counter
            KEY_COUNTER=$((KEY_COUNTER + 1))

            # Log progress every 5000 keys
            if (( KEY_COUNTER % 5000 == 0 )); then
                echo "Populated ${KEY_COUNTER} keys..."
            fi

        done
    done
done

echo "--- Data Population Complete ---"

echo "Verifying initial key counts:"
INITIAL_DBSIZE_7000=$(src/valkey-cli -p 7000 DBSIZE)
INITIAL_DBSIZE_7001=$(src/valkey-cli -p 7001 DBSIZE)
echo "Keys on 127.0.0.1:7000: ${INITIAL_DBSIZE_7000}"
echo "Keys on 127.0.0.1:7001: ${INITIAL_DBSIZE_7001}"


# Step 5: Make sure the keys are all there
KEY_COUNTER=0
for index in $(seq 1 $NUM_HASHS_PER_LETTER); do
    for letter in {A..Z}; do
        for i in $(seq 1 $NUM_KEYS_PER_HASH); do
            
            KEY="{hash${letter}${index}}key_$i"
            VALUE="value_${letter}_${index}_$i"
            
            # echo "Getting Key: ${KEY}. Expecting Val: ${VALUE}"
            # Send the SET command using the specified format
            ACTUAL_VALUE=$(src/valkey-cli -c -p 7001 get "${KEY}")

            # Compare the actual value with the expected value
            if [ "$ACTUAL_VALUE" == "$VALUE" ]; then
                : # Don't print on success
                # echo "  Key: ${KEY} - Value: ${ACTUAL_VALUE} (OK)"
            else
                echo "  Key: ${KEY} - MISMATCH! Expected: ${VALUE}, Got: ${ACTUAL_VALUE}"
                exit 1 # Exit if a mismatch is found
            fi

            
            KEY_COUNTER=$((KEY_COUNTER + 1))

            # Log progress every 5000 keys
            if (( KEY_COUNTER % 5000 == 0 )); then
                echo "Checked ${KEY_COUNTER} keys..."
            fi
        done
    done
done

# Step 6: SAVE DB 7000
echo "Running Save on Server 127.0.0.1:7000:"
src/valkey-cli -p 7000 SAVE
sleep 2 # Give it a moment to complete the SAVE operation


# Step 7: Close the server at port 7000
echo "--- Stopping server 127.0.0.1:7000 (PID: $PID_7000) ---"
kill "$PID_7000"
# Wait for the process to actually terminate
wait "$PID_7000" 2>/dev/null
echo "Server 7000 stopped."

# Step 8: Start the server back up (so it will reload the RDB file)
echo "--- Starting server 127.0.0.1:7000 again to reload RDB ---"
./src/valkey-server testconfs/valkey7000.conf & PID_7000_RELOADED=$!
echo "Reloading Valkey server 7000 PID: $PID_7000_RELOADED"
sleep 10 # Give the server time to start and reload the RDB file and re-join the cluster.
echo "Done Reloading!"


# Step 8: Confirm the number of keys on the servers is the same
src/valkey-cli -c -p 7000 DBSIZE 

# Step 9: Make sure all the keys are present
# Capture DBSIZE after reload and compare
echo "Verifying key counts after server reload:"
FINAL_DBSIZE_7000=$(src/valkey-cli -p 7000 DBSIZE)
FINAL_DBSIZE_7001=$(src/valkey-cli -p 7001 DBSIZE)

echo "Keys on 127.0.0.1:7000: ${FINAL_DBSIZE_7000} (after reload)"
echo "Keys on 127.0.0.1:7001: ${FINAL_DBSIZE_7001} (continuous)"

echo "--- Verifying keys after RDB reload on 127.0.0.1:7000 ---"

KEY_COUNTER=0
SUCCESS=0
for index in $(seq 1 $NUM_HASHS_PER_LETTER); do
    for letter in {A..Z}; do
        for i in $(seq 1 $NUM_KEYS_PER_HASH); do
            KEY="{hash${letter}${index}}key_$i"
            VALUE="value_${letter}_${index}_$i"
            
            ACTUAL_VALUE=$(src/valkey-cli -c -p 7000 get "${KEY}") # Connect to the reloaded 7000

            if [ "$ACTUAL_VALUE" == "$VALUE" ]; then
                # echo "  Key: ${KEY} - Value: ${ACTUAL_VALUE} (OK)" # Uncomment for verbose check
                : # Do nothing, continue loop
            else
                echo "  Key: ${KEY} - MISMATCH! Expected: ${VALUE}, Got: ${ACTUAL_VALUE}"
                SUCCESS=1 # Set flag for failure
                # Don't exit immediately, let's see all mismatches
            fi
            
            KEY_COUNTER=$((KEY_COUNTER + 1))
            # Log progress every 5000 keys
            if (( KEY_COUNTER % 5000 == 0 )); then
                echo "Checked ${KEY_COUNTER} keys..."
            fi
        done
    done
done



# --- Final Comparison of DBSIZE ---
echo "--- Final DBSIZE Comparison ---"
TEST_OVERALL_SUCCESS=0

if [ "$FINAL_DBSIZE_7000" -eq "$INITIAL_DBSIZE_7000" ] && \
   [ "$FINAL_DBSIZE_7001" -eq "$INITIAL_DBSIZE_7001" ]; then
    echo "DBSIZE counts are consistent across both servers after reload. (${FINAL_DBSIZE_7000} and ${FINAL_DBSIZE_7001})"
else
    echo "DBSIZE count MISMATCH detected!"
    echo "Initial 7000: ${INITIAL_DBSIZE_7000}, Final 7000: ${FINAL_DBSIZE_7000}"
    echo "Initial 7001: ${INITIAL_DBSIZE_7001}, Final 7001: ${FINAL_DBSIZE_7001}"
    TEST_OVERALL_SUCCESS=1
fi

if [ $VERIFICATION_SUCCESS_RELOAD -eq 0 ] && [ $TEST_OVERALL_SUCCESS -eq 0 ]; then
    echo "--- All persistence tests PASSED successfully! ---"
    FINAL_EXIT_CODE=0
else
    echo "--- Some persistence tests FAILED. Review output above. ---"
    FINAL_EXIT_CODE=1
fi


# Cleanup: Always good to kill background processes at the end of the script
echo "--- Cleaning up Valkey servers ---"
kill "$PID_7000_RELOADED" 2>/dev/null
kill "$PID_7001" 2>/dev/null
wait "$PID_7000_RELOADED" 2>/dev/null
wait "$PID_7001" 2>/dev/null
echo "All Valkey servers stopped."

exit $FINAL_EXIT_CODE # Exit with 0 for success, 1 for failure