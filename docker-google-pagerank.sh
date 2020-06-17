#!/bin/sh
docker exec -it master spark-submit \
--class part1.Wiki \
--executor-memory 512m \
--total-executor-cores 2 \
/root/jars/lab1.jar \
pagerank \
web-Google.txt
