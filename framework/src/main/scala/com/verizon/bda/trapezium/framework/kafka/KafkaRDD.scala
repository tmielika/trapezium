package com.verizon.bda.trapezium.framework.kafka

import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig, WorkflowConfig}
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}
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

       val param = getkafkaParam(appConfig, kafkaTopicName)
       val value = KafkaUtils.createRDD[String, String](sparkContext, param,
         offsetRangeList.toArray, LocationStrategies.PreferConsistent)

//     Convert to the (key , Value ) pair for backwards compatibility
       val record = value.map ( record => (record.key, record.value) )

       Some( record , fromOffset)

     } else {
       None
     }
  }


  def getkafkaParam (appConfig: ApplicationConfig, kafkaTopicName: String) : java.util.Map[String, Object] = {
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
    val kafkaParamsMap = new java.util.HashMap[String, Object]()
    kafkaParamsMap.put("bootstrap.servers",kafkaParamBootStrap)
    kafkaParamsMap.put("group.id" , appConfig.persistSchema)
    kafkaParamsMap.put("value.deserializer", classOf[StringDeserializer].getName)
    kafkaParamsMap.put("key.deserializer", classOf[StringDeserializer].getName)
    kafkaParamsMap
  }

}
