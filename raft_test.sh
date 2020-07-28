#!/bin/bash
set -o errexit
trap 'echo "Aborting due to errexit on line $LINENO. Exit code: $?" >&2' ERR
set -o errtrace
set -o pipefail
IFS=$'\n\t'

dirName="$(cd "$(dirname "$1")"; pwd -P)"
logFile="$dirName/test.log"

echo "[INFO] Starting test bench"
echo "[INFO] Errors will be logged to $logFile"

b1="TestBasicAgree2B"
b2="TestRPCBytes2B"
b3="TestFailAgree2B"
b4="TestFailNoAgree2B"
b5="TestConcurrentStarts2B"
b6="TestRejoin2B"
b7="TestBackup2B"

# Empty the file
echo "" >> test.log

cd src/raft

for i in {1..100}; do
        echo "[INFO] Test Run $i"
        go test -race -run $b7 1>$logFile 2>&1
        if [[ $? -ne 0 ]]; then
                echo "[ERROR] Test case failed. Exiting..."
                break
        fi
        # cleanup logfile
        echo "" >> $logFile
done
