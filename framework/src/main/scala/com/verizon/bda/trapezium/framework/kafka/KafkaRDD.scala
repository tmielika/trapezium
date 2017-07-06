package com.verizon.bda.trapezium.framework.kafka

import com.verizon.bda.trapezium.framework.manager.{WorkflowConfig, ApplicationConfig}
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import org.apache.spark.rdd.RDD
import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.{OffsetRange, HasOffsetRanges, KafkaUtils}
import kafka.serializer.StringDecoder
import org.slf4j.LoggerFactory

/**
  * Created by v708178 on 6/6/17.
  */
private[framework] object KafkaRDD {

  def getRDDFromKafka ( kafkaTopicName: String,
                        appConfig: ApplicationConfig,
                        workflowConfig: WorkflowConfig,
                        sparkContext: SparkContext) : Option[(RDD[(String, String)], Long)] = {
    val logger = LoggerFactory.getLogger(this.getClass)
    logger.info ("inside")
    val zk = ZooKeeperConnection.create(appConfig.zookeeperList)
    logger.info ("zk connection")
    val zkpath = ApplicationUtils.
      getCurrentWorkflowKafkaPath(appConfig, workflowConfig.workflow, workflowConfig)
    logger.info ("zk zkpath" + zkpath)
    val offsetRangeList: List[OffsetRange] =
      KafkaUtil.getOffsetsRange(zk, zkpath, kafkaTopicName, appConfig)
    logger.info ("got offset" + offsetRangeList.size)
     if (offsetRangeList.size>0) {
       val fromOffset = offsetRangeList(0).fromOffset
       logger.info ("zk fromOffset" + fromOffset)
      Some( KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](sparkContext,
         getkafkaParam(appConfig, kafkaTopicName), offsetRangeList.toArray) , fromOffset)
     } else {
       None
     }
  }


  def getkafkaParam (appConfig: ApplicationConfig, kafkaTopicName: String) : Map[String, String] = {
    val logger = LoggerFactory.getLogger(this.getClass)
    val kafkaConfParam = appConfig.kafkaConfParam

    val kafkaParamBootStrap = {
      if (appConfig.getEnv().equals("local")) {
        KafkaApplicationUtils.kafkaBrokers
      } else {
        kafkaConfParam.getString("bootstrap.servers")
      }
    }
    logger.info ("KafkaApplicationUtils.kafkaBrokers" + kafkaParamBootStrap)
    val kafkaParamsMap = Map[String, String](
      "bootstrap.servers" -> kafkaParamBootStrap,
      "group.id" -> appConfig.persistSchema
    )
    kafkaParamsMap
  }






}
