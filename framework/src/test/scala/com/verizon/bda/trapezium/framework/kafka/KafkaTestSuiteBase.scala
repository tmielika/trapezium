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

import java.io.File
import java.net.ServerSocket
import java.util.Properties

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig, WorkflowConfig}
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import kafka.common.KafkaException
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.commons.io
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.zookeeper.EmbeddedZookeeper
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.LoggerFactory

/**
 * @author Pankaj on 3/8/16.
 */
trait KafkaTestSuiteBase extends FunSuite with BeforeAndAfter {
  val kf_logger = LoggerFactory.getLogger(this.getClass)
  private var zkList: String = _
  var zk_kafka: EmbeddedZookeeper = _
  private val zkConnectionTimeout = 6000
  private val zkSessionTimeout = 6000
  private var kafkaBrokers: String = _
  private var brokerConf: KafkaConfig = _
  private var server: KafkaServer = _
  private var server2: KafkaServer = _
  private var brokerPort2 = 9093
  private var brokerConf2: KafkaConfig = _
  private var producer: KafkaProducer[String, String] = _
  private var zkReady = false
  private var brokerReady = false

  protected var zkClient: ZkClient = _
  protected var utils: KafkaApplicationUtils = _

  before {

    // Load the config file
    val appConfig = ApplicationManager.getConfig()
    kafkaBrokers = appConfig.kafkabrokerList
    zkList = appConfig.zookeeperList

    kf_logger.info("Kafka broker list " + kafkaBrokers)

    // set up local Kafka cluster
    setupKafka
     utils = getZKUtils
  }

  after {
    tearDownKafka
  }


  private def zkAddress: String = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper address")
    s"$zkList"
  }

  private def brokerAddress: String = {
    assert(brokerReady, "Kafka not setup yet or already torn down, cannot get broker address")
    s"$kafkaBrokers"
  }

  private def setupKafka() {

    // Zookeeper server startup
    zk_kafka = new EmbeddedZookeeper(s"$zkList")

    // for local as well as jenkins build
    if (ApplicationManager.getConfig().env == "local" ) {

      // Use port that is available
      zkList = EmbeddedZookeeper.zkConnectString
    }

    // Get the actual zookeeper binding port
    zkReady = true
    kf_logger.info("==================== Zookeeper Started ====================")

    zkClient = ZkUtils.createZkClient(zkAddress, zkSessionTimeout, zkConnectionTimeout)

    kf_logger.info("==================== Zookeeper Client Created ====================")

    // Kafka broker startup
    var bindSuccess: Boolean = false
    while (!bindSuccess) {
      try {
        val brokerProps = getBrokerConfig()
        brokerConf = new KafkaConfig(brokerProps)
        server = new KafkaServer(brokerConf)
        server.startup()
        kf_logger.info("==================== Kafka Broker Started ====================")
        bindSuccess = true
      } catch {
        case e: KafkaException =>
          throw new Exception("Socket server failed to bind to", e)
        /*
        if (e.getMessage != null && e.getMessage.contains("Socket server failed to bind to")) {
          brokerPort += 1
        }
        */
        case e: Exception => throw new Exception("Kafka server create failed", e)
      }
    }

    Thread.sleep(1000)
    kf_logger.info("==================== Kafka + Zookeeper Ready ====================")
    brokerReady = true
  }

  // Method added to enable testing of the broker failure recovery
  private def justShutdownBroker(): Unit = {
    server.shutdown()
  }

  private def startSecondBroker(): Unit = {
    var bindSuccess: Boolean = false
    while (!bindSuccess) {
      try {
        val brokerProps = getBrokerConfig()
        brokerProps.put("broker.id", "1")
        brokerProps.put("port", brokerPort2.toString)
        brokerProps.put("log.dir", "target/kafka")

        brokerConf2 = new KafkaConfig(brokerProps)
        server2 = new KafkaServer(brokerConf2)
        server2.startup()
        kf_logger.info("==================== Second Kafka Broker Started ====================")
        bindSuccess = true
      } catch {
        case e: KafkaException =>
          if (e.getMessage != null && e.getMessage.contains("Socket server failed to bind to")) {
            brokerPort2 += 1
          }
        case e: Exception => throw new Exception("Kafka server create failed", e)
      }
    }
  }

  // Method added to enable testing of the broker failure recovery
  private def restartBroker(): Unit = {
    var bindSuccess: Boolean = false
    while (!bindSuccess) {
      try {
        /*
        val brokerProps = getBrokerConfig()
        brokerConf = new KafkaConfig(brokerProps)
        server = new KafkaServer(brokerConf)
        */
        server.startup()
        kf_logger.info("==================== Kafka Broker Re-Started ====================")
        bindSuccess = true
      } catch {
        case e: KafkaException =>
          throw new Exception("Socket server failed to bind to", e)
        /*
        if (e.getMessage != null && e.getMessage.contains("Socket server failed to bind to")) {
          brokerPort += 1
        }
        */
        case e: Exception => throw new Exception("Kafka server create failed", e)
      }
    }

    Thread.sleep(1000)
    kf_logger.info("==================== Kafka + Zookeeper Ready Again ====================")
    brokerReady = true
  }

  private def tearDownKafka() {
    brokerReady = false
    zkReady = false
    if (producer != null) {
      producer.close()
      producer = null
    }

    if (server != null) {
      server.shutdown()
      server = null
    }
    if (server2 != null) {
      server2.shutdown()
      server2 = null
    }

    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }

    // Close ZooKeeper connections
    ZooKeeperConnection.close

    // shutdown zookeeper
    if (zk_kafka != null) {
      zk_kafka.shutdown()
      zk_kafka = null
    }

  }

  private[framework] def sendMessages(topic: String, messageToFreq: Map[String, Int]) {
    val messages = messageToFreq.flatMap { case (s, freq) => Seq.fill(freq)(s) }.toArray
    sendMessages(topic, messages)
  }


  private[framework] def sendMessages(topic: String, messages: Array[String]) {
    producer = new KafkaProducer[String, String](getProducerConfig())
    // producer.send(messages.map { new KeyedMessage[String, String](topic, _ ) }: _*)

    sendMessages(producer, topic, messages)

    producer.close()
    kf_logger.info(s"=============== Sent Messages ===================")
  }

  private[framework]  def sendMessages(producer: KafkaProducer[String,String], topic: String, messages: Array[String]) = {
    for (msg <- messages)
      producer.send(
        new org.apache.kafka.clients.producer.ProducerRecord[String, String](topic, null, msg)
      )
  }

  private def deleteRecursively(in : File): Unit = {

    if ( in.isDirectory ) {
      io.FileUtils.deleteDirectory(in)
    } else {
      in.delete()
    }
  }

  private def getBrokerConfig(): Properties = {
    val kafkaBrokerList = kafkaBrokers.split(",")
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("host.name", kafkaBrokerList(0).split(":")(0))

    // for local as well as jenkins build
    if (ApplicationManager.getConfig().env == "local" ) {

      val socket = new ServerSocket(0)
      val tempPort = socket.getLocalPort

      // closing the socket
      socket.close()
      props.put("port", s"$tempPort")

      kafkaBrokers = s"${kafkaBrokerList(0).split(":")(0)}:${tempPort}"

      // this is needed so that App Mgr code has access to new port
      KafkaApplicationUtils.kafkaBrokers = kafkaBrokers
    } else {

      props.put("port", kafkaBrokerList(0).split(":")(1))
    }


    deleteRecursively( new File("target/kafka"))
    props.put("log.dir", "target/kafka")
    props.put("zookeeper.connect", zkAddress)
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
    props
  }

  /*
   private def waitUntilMetadataIsPropagated(topic: String, partition: Int) {
     eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
       assert(
         server.apis.metadataCache.containsTopicAndPartition(topic, partition),
         s"Partition [$topic, $partition] metadata not propagated after timeout"
       )
     }
   }
   */

  private[framework] def getProducerConfig(): Properties = {
    var brokerAddr = brokerConf.hostName + ":" + brokerConf.port
    if (brokerConf2 != null) brokerAddr += "," + brokerConf2.hostName + ":" + brokerConf2.port
    val props = new Properties()

    props.put("bootstrap.servers", brokerAddr)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("key.serializer", classOf[StringSerializer].getName)

    props
  }
  /**
    * create topic
    * @param topic
    * @param nparts
    */
  def createTopic(topic: String, nparts: Int = 1): Unit = {

    utils.createTopic(topic, nparts)
  }

  private def getZKUtils: KafkaApplicationUtils = {
//    TODO: Switch to this API in the near future
//    ZkUtils.createZkClientAndConnection(zkList , 100, 100)
    val zkUtils: ZkUtils = new ZkUtils(zkClient, new ZkConnection(zkList), false)
    val utils = new KafkaApplicationUtils(zkUtils, kafkaBrokers)
    utils
  }

  def setupWorkflow(workflowName: String, inputSeq: Seq[Seq[String]],
                    adapt: (WorkflowConfig, ApplicationConfig) => Unit = null): Unit = {

    val workflowConfig = ApplicationManager.setWorkflowConfig(workflowName)

    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]
    val streamsInfo = kafkaConfig.getConfigList("streamsInfo")

    val topicName = streamsInfo.get(0).getString("topicName")
    val newInputSeq = inputSeq.map(seq => Seq((topicName, seq)))

    setupWorkflowForMultipleTopics(workflowName, newInputSeq, adapt)

  }

  def setupWorkflowForMultipleTopics(workflowName: String,
                                     inputSeq: Seq[Seq[(String, Seq[String])]],
                                     adapt: (WorkflowConfig, ApplicationConfig) => Unit = null): Unit = {

    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.setWorkflowConfig(workflowName)

    if(adapt!=null)
      adapt(workflowConfig, appConfig)

    // create topcis
    utils.createTopics(workflowConfig)
    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]

    val currentTimeStamp = System.currentTimeMillis()
    ApplicationManager.updateWorkflowTime(currentTimeStamp)

    startApplication(inputSeq, workflowConfig, kafkaConfig, appConfig)

  }

  private[framework] def startApplication(inputSeq: Seq[Seq[(String, Seq[String])]], workflowConfig: WorkflowConfig, kafkaConfig: Config, appConfig: ApplicationConfig) = {
    val sparkConf = ApplicationManager.getSparkConf(appConfig)
    val ssc = KafkaDStream.createStreamingContext(sparkConf)

    utils.startKafkaWorkflow(workflowConfig, ssc)

    // start streaming
    ssc.start

    inputSeq.foreach(input => {

      input.foreach(seq => {

        kf_logger.info(s"OLD Size of the input: ${seq._2.size}")
        sendMessages(seq._1, seq._2.toArray)

      })

      Thread.sleep(kafkaConfig.getLong("batchTime") * 1000)

    })

    ssc.awaitTerminationOrTimeout(
      kafkaConfig.getLong("batchTime") * 1000)

    if (ssc != null) {
      kf_logger.info("Stopping streaming context from test Thread.")
      ssc.stop(true, false)

      // reset option
      KafkaDStream.sparkcontext = None
    }

    assert(!ApplicationManager.stopStreaming)
  }

  /**
   * Added method to test multiple kafka workflows reading from multiple Kafka topics
 *
   * @param workflowNames
   * @param inputSeq
   */
  def setupMultipleWorkflowForMultipleTopics(workflowNames: List[String],
                                     inputSeq: Seq[Seq[(String, Seq[String])]],
                                      adapt: (WorkflowConfig, ApplicationConfig) => Unit = null): Unit = {

    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.setWorkflowConfig(workflowNames(0))

    if(adapt!=null)
      adapt(workflowConfig,appConfig)

    // create topcis
    utils.createTopics(workflowConfig)
    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]

    val currentTimeStamp = System.currentTimeMillis()
    ApplicationManager.updateWorkflowTime(currentTimeStamp)

    val sparkConf = ApplicationManager.getSparkConf(appConfig)
    val ssc = KafkaDStream.createStreamingContext(sparkConf)

    workflowNames.foreach( workflowName => {
      val thread = new KafkaWorkflowThread(workflowName, ssc, utils)
      thread.start()
      do{

        kf_logger.info(s"Waiting for workflow ${workflowName} to start...")
        Thread.sleep(1000)
      }while (!thread.isStarted)
    })

    // start streaming
    ssc.start

    inputSeq.foreach( input => {

      input.foreach( seq => {

        kf_logger.info(s"OLDER Size of the input: ${seq._2.size}")
        sendMessages(seq._1, seq._2.toArray)

      })

      Thread.sleep(kafkaConfig.getLong("batchTime") * 1000)

    })

    ssc.awaitTerminationOrTimeout(
      kafkaConfig.getLong("batchTime") * 1000)

    if( ssc != null ) {
      kf_logger.info(s"Stopping streaming context from test Thread.")
      ssc.stop(true, false)

      // reset option
      KafkaDStream.sparkcontext = None
    }

    assert (!ApplicationManager.stopStreaming)

  }

}

class KafkaWorkflowThread (workflowName: String,
                           ssc: StreamingContext,
                           utils: KafkaApplicationUtils) extends Thread {

  var isStarted: Boolean = false

  override def run(): Unit = {

    val workflowConfig = ApplicationManager.setWorkflowConfig(workflowName)
    utils.startKafkaWorkflow(workflowConfig, ssc)

    isStarted = true
  }

}
