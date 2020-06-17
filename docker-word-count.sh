#!/bin/sh
docker exec -it master spark-submit \
--class com.bolingcavalry.sparkwordcount.WordCount \
--executor-memory 512m \
--total-executor-cores 2 \
/root/jars/sparkwordcount.jar \
namenode \
8020 \
web-Google.txt
