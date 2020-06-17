#!/bin/bash
docker exec -it master spark-shell --executor-memory 512M --total-executor-cores 2
