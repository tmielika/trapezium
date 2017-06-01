/**
* Copyright (C) 2016 Verizon. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.verizon.bda.trapezium.framework.kafka

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.{WorkflowConfig, ApplicationListener}
import kafka.admin.AdminUtils
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.{MutableList => MList}
import scala.collection.mutable.{Map => MMap}
import org.I0Itec.zkclient.ZkClient


/**
 * Created by Pankaj on 2/17/17.
 */
class KafkaApplicationUtils(zkClient: ZkClient, kafkaBrokers: String) {
  val logger = LoggerFactory.getLogger(this.getClass)
  /**
   * start Kafka workflow
   * @param workflowConfig
   * @param ssc
   */
  def startKafkaWorkflow(workflowConfig: WorkflowConfig, ssc: StreamingContext): Unit = {

    // Load the config file
    val applicationConfig = ApplicationManager.getConfig()

    val runMode = workflowConfig.runMode
    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]

    val topicPartitionOffsets = MMap[TopicAndPartition, Long]()

    val streamsInfo = kafkaConfig.getConfigList("streamsInfo")
    streamsInfo.asScala.foreach( streamInfo => {

      val topicName = streamInfo.getString("topicName")

      val partitionOffset =
        KafkaDStream.fetchPartitionOffsets(topicName, runMode, applicationConfig)
      topicPartitionOffsets ++= partitionOffset
    })

    val dStreams = KafkaDStream.createDStreams(ssc, kafkaBrokers,
      kafkaConfig, topicPartitionOffsets.toMap, applicationConfig)

    ApplicationManager.startStreamWorkFlow(dStreams, workflowConfig)

    // add streaming listener
    val listener: ApplicationListener = new ApplicationListener(workflowConfig)
    ssc.addStreamingListener(listener)

  }

  /**
   * get list of topics from workflow and create topics if they do not exist
   * @param workflowConfig
   */
  def createTopics(workflowConfig: WorkflowConfig): Unit = {

    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]
    val streamsInfo = kafkaConfig.getConfigList("streamsInfo")

    val topics: MList[String] = MList()
    streamsInfo.asScala.foreach(streamInfo => {

      val topicName = streamInfo.getString("topicName")
      logger.debug("Kafka stream topic - " + topicName)

      topics += topicName
      createTopic(topicName)

    })
  }

  /**
   * create Kafka topic
   * @param topic
   * @param nparts
   */
  def createTopic(topic: String, nparts: Int = 1) {
    if (!AdminUtils.topicExists(zkClient, topic)) {

      AdminUtils.createTopic(zkClient, topic, nparts, 1)
      // wait until metadata is propagated
      // waitUntilMetadataIsPropagated(topic, 0)
      logger.info(s"==================== Topic $topic Created ====================")

    } else {

      logger.info(s"================= Topic $topic already exists ================")
    }
  }

}

private[framework] object KafkaApplicationUtils {

  var kafkaBrokers: String = _
}
