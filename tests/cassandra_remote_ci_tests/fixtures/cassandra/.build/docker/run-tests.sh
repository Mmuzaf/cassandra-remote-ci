#!/bin/bash
# Trimmed from apache/cassandra .build/docker/run-tests.sh: the lines the limits patch rewrites.
jenkins_executors=1
docker_flags="-m 5g --memory-swap 5g"
docker_flags="-m 15g --memory-swap 15g"
docker_cpus=$(echo "scale=2; ${cores} / ( ${jenkins_executors} )" | bc)
