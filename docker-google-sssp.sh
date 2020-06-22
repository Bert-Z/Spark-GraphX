#!/bin/sh
docker exec -it master spark-submit \
--class lab.Google \
--executor-memory 512m \
--total-executor-cores 2 \
/root/jars/Graphx_jar/Graphx.jar \
sssp \
hdfs://namenode:8020/input/web-Google.txt \
--numEPart=1000 > google-sssp.log
