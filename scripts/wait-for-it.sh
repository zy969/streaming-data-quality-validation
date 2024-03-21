#!/usr/bin/env bash

HOST="$1"
PORT="$2"
TIMEOUT="${3:-60}"

# Wait for a TCP host/port to be available
echo "Waiting for ${HOST}:${PORT} to become available..."

for i in $(seq $TIMEOUT) ; do
    nc -z "$HOST" "$PORT" > /dev/null 2>&1
    result=$?
    if [ $result -eq 0 ] ; then
        echo "${HOST}:${PORT} is available after ${i} seconds"
        exit 0
    fi
    sleep 1
done

echo "Timeout of ${TIMEOUT} seconds reached, exiting with error"
exit 1
