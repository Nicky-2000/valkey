#!/bin/bash

# Configuration
NUM_HASHS_PER_LETTER=10
NUM_KEYS_PER_HASH=10

echo "--- Populating ValKey Cluster ---"

# Loop through each letter of the alphabet (A to Z)
for index in $(seq 1 $NUM_HASHS_PER_LETTER); do
    for letter in {A..Z}; do
        echo "--- Adding {hash${letter}${index}} keys ---"
        for i in $(seq 1 $NUM_KEYS_PER_HASH); do
            KEY="{hash${letter}${index}}key_$i"
            VALUE="value_${letter}_${index}_$i"
            
            # Send the SET command using the specified format
            src/valkey-cli -p 7000 set "${KEY}" "${VALUE}"
        done
    done
done

echo "--- Data population complete. ---"
echo "Total expected keys: $(( ${NUM_HASHS_PER_LETTER} * ${NUM_KEYS_PER_HASH} * 26 ))"

# Optional: Verify key counts on each server
echo "Verifying key counts:"
echo -n "Keys on 127.0.0.1:7000: "
src/valkey-cli -p 7000 DBSIZE
src/valkey-cli -p 7000
