#!/bin/bash

printf "yes\nyes\n" | src/valkey-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 --cluster-replicas 0

sleep 5

./populate_servers.sh

src/valkey-cli -c -p 7000