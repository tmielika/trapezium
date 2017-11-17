/**
  * Copyright (C) 2016 Verizon. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.verizon.bda.trapezium.framework.kafka

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig, WorkflowConfig}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.streaming.scheduler._
import org.slf4j.LoggerFactory

/**
  * Sends messages and finds out the count of messages
  */
class KafkaHATestSuiteBase extends KafkaTestSuiteBase {


   def repeatMessageSendCount : Int  = 1

  override  private[framework] def startApplication(inputSeq: Seq[Seq[(String, Seq[String])]], workflowConfig: WorkflowConfig,
                                                    kafkaConfig: Config, appConfig: ApplicationConfig,
                                                    testCondition: (WorkflowConfig, ApplicationConfig, Int) =>  (Conditionality) ) = {

    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]
    val streamsInfo = kafkaConfig.getConfigList("streamsInfo")
    val topicName = streamsInfo.get(0).getString("topicName")

//   Create 6 partitions
    createTopic(topicName , 6)

    /**
      * Allow multiple contexts to be started in the same JVM
      */
    val sparkConf1 = ApplicationManager.getSparkConf(appConfig)
    sparkConf1.set("spark.driver.allowMultipleContexts", "true")

    val ssc1 = KafkaDStream.createStreamingContext(sparkConf1)

    var count = 0

    /**
      * collect the count of messages
      */
    inputSeq.foreach(input => {
      input.foreach(seq => {
        count = count + seq._2.toArray.length
      })
    })

    val producer = new KafkaProducer[String, String](getProducerConfig())

    val totalCount  = count * repeatMessageSendCount

    var a = 0

    def send(batch : Int , input: Seq[(String, Seq[String])]) = {
      input.foreach(seq => {
        kf_logger.info(s"[${batch}] Size of the input: ${seq._2.size} ")
        sendMessages(producer, seq._1, seq._2.toArray)
      })
    }

    for(a <- 1 to repeatMessageSendCount) {
      inputSeq.foreach(input => {
        send(a, input)
      })
    }

    producer.close()

    kf_logger.info(s"-- SENT ALL MESSAGES [count] = ${totalCount} ---")

    val latch = new CountDownLatch(totalCount)

    val streamingListener = new SimpleCountingListener(latch)

    ssc1.addStreamingListener(streamingListener)

    // start streaming
    utils.startKafkaWorkflow(workflowConfig, ssc1)

    kf_logger.info(s"ssc1 status  = ${ssc1.getState()}")

    ssc1.start


    val waitTime = kafkaConfig.getLong("batchTime") * 3 * 10
    kf_logger.info(s"Lacth awaiting for termination  on ${latch} @ ${latch.getCount} for ${waitTime}")

    /**
      * wait till the process of consumption is completed
      */
    latch.await(getWaitTime(waitTime), TimeUnit.SECONDS)

    if(latch.getCount!=0) {
      ssc1.awaitTerminationOrTimeout(
        kafkaConfig.getLong("batchTime") * 1000)
    }

    kf_logger.info("Stopping the streaming context")
    ssc1.stop(true, false)

    // reset option
    KafkaDStream.sparkcontext = None

    kf_logger.info(s"Latch BATCH_COUNT on  on ${latch} =  ${latch.getCount}")
    assert(latch.getCount == 0, s"Expected count =${totalCount} and received count = ${totalCount-latch.getCount}")

    if(ApplicationManager.stopStreaming)
      fail(s"stopStreaming is true ${ApplicationManager.throwable}", ApplicationManager.throwable)

  }


  def getWaitTime(waitTime: Long) = {
    waitTime + (repeatMessageSendCount / 10)
  }
}


class SimpleCountingListener(latch: CountDownLatch) extends StreamingListener {
  val logger = LoggerFactory.getLogger(this.getClass)

  /** Called when processing of a batch of jobs has completed. */
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    var count = batchCompleted.batchInfo.numRecords
    logger.info(s"BATCH_ID SIZE [Before] on ${latch} = ${count}")
    while (count > 0) {
      latch.countDown()
      count -= 1
    }
    logger.info(s"BATCH_ID SIZE [After] on ${latch} = ${count} and @latch = ${latch.getCount}")
  }
}