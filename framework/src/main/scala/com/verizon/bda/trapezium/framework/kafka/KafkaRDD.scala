package com.verizon.bda.trapezium.framework.kafka

import com.verizon.bda.trapezium.framework.manager.{WorkflowConfig, ApplicationConfig}
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import org.apache.spark.rdd.RDD
import com.typesafe.config.Config
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.{OffsetRange, HasOffsetRanges, KafkaUtils}
import kafka.serializer.StringDecoder
/**
  * Created by v708178 on 6/6/17.
  */
private[framework] object KafkaRDD {

  def getRDDFromKafka ( kafkaTopicName: String,
                        appConfig: ApplicationConfig,
                        workflowConfig: WorkflowConfig,
                        sparkContext: SparkContext) : RDD[(String, String)] = {
    val zk = ZooKeeperConnection.create(appConfig.zookeeperList)
    val zkpath = ApplicationUtils.
      getCurrentWorkflowKafkaPath(appConfig, workflowConfig.workflow, workflowConfig)

    val offsetRangeList: List[OffsetRange] =
      KafkaUtil.getOffsetsRange(zk, zkpath, kafkaTopicName, appConfig)
     KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](sparkContext,
      getkafkaParam(appConfig.kafkaConfParam, kafkaTopicName), offsetRangeList.toArray)
  }


  def getkafkaParam (kafkaConfParam: Config, kafkaTopicName: String) : Map[String, String] = {
    val groupid = kafkaTopicName + "groupid"
    val kafkaParamsMap = Map[String, String](
      "bootstrap.servers" -> KafkaApplicationUtils.kafkaBrokers,
      "group.id" -> "testGroup"
    )
    kafkaParamsMap
  }






}
