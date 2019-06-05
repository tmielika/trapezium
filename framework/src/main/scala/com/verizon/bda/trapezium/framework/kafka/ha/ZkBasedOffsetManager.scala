package com.verizon.bda.trapezium.framework.kafka.ha

import com.verizon.bda.trapezium.framework.kafka.KafkaDStream
import com.verizon.bda.trapezium.framework.kafka.consumer.{IOffsetManager, Offsets}
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import org.slf4j.LoggerFactory

/**
  * An offset manager that looks up from the Zookeeper to fetch the offsets
  */
class ZkBasedOffsetManager(appConfig: ApplicationConfig,  workflowName: String, syncWorkflow: String) extends IOffsetManager {

  def getOffsets(kafkaTopicName: String): Offsets = {
    val logger = LoggerFactory.getLogger(this.getClass)
    var offsetMap: Map[Int, Long] = Map()
    logger.debug(s"fetching offsets from Zk for topic ${kafkaTopicName}")

    val currentWorkflowKafkaPath =
      ApplicationUtils.getCurrentWorkflowKafkaPath(appConfig, kafkaTopicName, workflowName)

    val dependentWorkflowKafkaPath =
      ApplicationUtils.getDependentWorkflowKafkaPath(appConfig, kafkaTopicName, syncWorkflow)

    val partitionOffsets = KafkaDStream.fetchPartitionOffsets(kafkaTopicName, appConfig,dependentWorkflowKafkaPath,currentWorkflowKafkaPath)
    for ((topic, offset) <- partitionOffsets) offsetMap += (topic.partition() -> offset)
    new Offsets(offsetMap)
  }
}
