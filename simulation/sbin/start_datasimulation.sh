#!/bin/bash
export YARN_CONF_DIR=/etc/hadoop/conf

echo $HADOOP_CONF_DIR

/opt/bda/spark-1.6.0/bin/spark-submit --master yarn --num-executors 2 --driver-memory 4g --executor-memory 16g --executor-cores 2 --jars commons-simulation-0.3.6-SNAPSHOT-jar-with-dependencies.jar --conf spark.yarn.executor.memoryOverhead=4000 --class com.verizon.bda.commons.framework.ApplicationManager commons-simulation-0.3.6-SNAPSHOT.jar --config conf --workflow datasimulationFlow
