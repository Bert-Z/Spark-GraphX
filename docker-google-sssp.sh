###
 # @Author: your name
 # @Date: 2020-06-22 16:16:43
 # @LastEditTime: 2020-06-22 16:16:44
 # @LastEditors: your name
 # @Description: In User Settings Edit
 # @FilePath: /CA/Spark-GraphX/docker-google-sssp.sh
### 
#!/bin/sh
docker exec -it master spark-submit \
--class lab.Google \
--executor-memory 512m \
--total-executor-cores 2 \
/root/jars/Graphx_jar/Graphx.jar \
sssp \
hdfs://namenode:8020/input/web-Google.txt \
--numEPart=100 > google-sssp.log
