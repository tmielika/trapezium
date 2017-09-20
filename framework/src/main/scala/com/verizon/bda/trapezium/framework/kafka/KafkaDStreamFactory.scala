package com.verizon.bda.trapezium.framework.kafka

import java.util.Properties

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.kafka.consumer.ConsumerConfig
import com.verizon.bda.trapezium.framework.kafka.custom.CustomKafkaSparkDStream
import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig, WorkflowConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.collection.mutable.{Map => MMap}
import scala.reflect.ClassTag

/**
  * A Factory that creates instance of Spark DStream for Kafka
  */
object KafkaDStreamFactory {

  val logger = LoggerFactory.getLogger(this.getClass)

  def createCustomKafkaSparkDStream[K: ClassTag, V: ClassTag](ssc: StreamingContext,
                                                              workflowConfig: WorkflowConfig,
                                                              kafkaConfig: Config,
                                                              appConfig: ApplicationConfig,
                                                              kafkaParams: MMap[String, Object],
                                                              topicName: String) : InputDStream[ConsumerRecord[K, V]] = {

    // Ensure no lazy config evaluations - fails serializing to executor
    val props : Properties = new Properties()
    kafkaParams.foreach( x => props.put(x._1, x._2))

    val topicSet = new java.util.HashSet[String]()
    topicSet.add(topicName.toString)

    val pollTime = workflowConfig.pollTime
    val waitBetweenPolls = workflowConfig.waitBetweenPolls
    val maxRecordSize = workflowConfig.maxRecordSize
    val kafkaConfig = new ConsumerConfig(props, topicSet, pollTime,waitBetweenPolls, maxRecordSize)

    CustomKafkaSparkDStream.createDStream(ssc,kafkaConfig, appConfig, workflowConfig.workflow, workflowConfig.syncWorkflow)
  }


  def createKafkaDirectStream[K: ClassTag, V: ClassTag](ssc: StreamingContext,
                                                        kafkaConfig: Config,
                                                        fromOffsets: Map[TopicPartition, Long],
                                                        appConfig: ApplicationConfig,
                                                        kafkaParams: MMap[String, Object],
                                                        topicName: String,
                                                        getTopicOffsets: (Map[TopicPartition, Long], String) => Map[TopicPartition, Long])
  : InputDStream[ConsumerRecord[K, V]] = {

    val topicSet = new scala.collection.mutable.HashSet[String]()

    topicSet += topicName

    if (fromOffsets.size > 0) {

      val topicOffsets: Map[TopicPartition, Long] = getTopicOffsets(fromOffsets, topicName)

      if (topicOffsets.size > 0) {

        val subscription = ConsumerStrategies.Subscribe[K, V](topicSet, kafkaParams, topicOffsets)

        val dStreamOffset = KafkaUtils.createDirectStream[K, V](ssc, LocationStrategies.PreferConsistent, subscription);

        return dStreamOffset

      } else {

//        logger.warn(s"No topic offset exists for ${topicName}. Starting streams as per KafkaParams")

        val subscription = ConsumerStrategies.Subscribe[K, V](topicSet, kafkaParams)

        val dStreamOffset = KafkaUtils.createDirectStream[K, V](ssc, LocationStrategies.PreferConsistent, subscription);

        return dStreamOffset
      }

    } else {

//      logger.warn(s"No offset found for ${topicName}. Starting streams as per KafkaParams")

      val subscription = ConsumerStrategies.Subscribe[K, V](topicSet, kafkaParams)

      val dStreamOffset = KafkaUtils.createDirectStream[K, V](ssc, LocationStrategies.PreferConsistent, subscription);

      return dStreamOffset

    }
  }


}
