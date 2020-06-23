#!/bin/sh
docker exec -it master spark-submit \
--class lab.Google \
--total-executor-cores 2 \
/root/jars/Graphx_jar/Graphx.jar \
pagerank \
hdfs://namenode:8020/input/web-Google.txt \
--numEPart=1000 > google-pagerank.log
