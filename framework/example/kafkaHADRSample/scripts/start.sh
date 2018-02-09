#!/bin/bash
export YARN_CONF_DIR=/etc/hadoop/conf

${SPARK_HOME}/bin/spark-submit \
 --master local[3] \
 --num-executors 20 \
 --driver-memory 16g \
 --executor-memory 16g \
 --executor-cores 16 \
 --jars target/KafkaStreaming-1.4.0-SNAPSHOT-jar-with-dependencies.jar \
 --class com.verizon.bda.trapezium.framework.ApplicationManager target/KafkaStreaming-1.4.0-SNAPSHOT-jar-with-dependencies.jar \
 --uid kafkaSample \
 --config conf \
 --workflow kafkaSample &

