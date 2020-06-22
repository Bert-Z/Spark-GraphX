#!/bin/sh
docker exec -it master spark-submit \
--class lab.Wiki \
--executor-memory 512m \
--total-executor-cores 2 \
/root/jars/wiki/Graphx.jar \
pagerank \
hdfs://namenode:8020/input/wiki-Vote.txt \
--numEPart=100 > wiki-pagerank.log
