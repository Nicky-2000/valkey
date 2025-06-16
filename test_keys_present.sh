#!/bin/bash

# Loop through each letter of the alphabet (A to Z)
for index in $(seq 1 $NUM_HASHS_PER_LETTER); do
    for letter in {A..Z}; do
        for i in $(seq 1 $NUM_KEYS_PER_HASH); do
            
            KEY="{hash${letter}${index}}key_$i"
            VALUE="value_${letter}_${index}_$i"
            
            
            # echo "Getting Key: ${KEY}. Expecting Val: ${VALUE}"
            # Send the SET command using the specified format
            ACTUAL_VALUE=$(src/valkey-cli -c -p 7000 get "${KEY}")

            # Compare the actual value with the expected value
            if [ "$ACTUAL_VALUE" == "$VALUE" ]; then
                echo "  Key: ${KEY} - Value: ${ACTUAL_VALUE} (OK)"
            else
                echo "  Key: ${KEY} - MISMATCH! Expected: ${VALUE}, Got: ${ACTUAL_VALUE}"
                exit 1 # Exit if a mismatch is found
            fi
        done
    done
done