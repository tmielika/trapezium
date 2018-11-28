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
package com.verizon.bda.trapezium.framework

import org.apache.kafka.common.TopicPartition
import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.handler.{BatchHandler, FileSourceGenerator, StreamingHandler}
import com.verizon.bda.trapezium.framework.hdfs.HdfsDStream
import com.verizon.bda.trapezium.framework.kafka.{KafkaApplicationUtils, KafkaDStream}
import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig, WorkflowConfig}
import com.verizon.bda.trapezium.framework.server._
import com.verizon.bda.trapezium.framework.utils.{ApplicationUtils, HttpsConnectionContextBuilder}
import com.verizon.bda.license.{LicenseException, LicenseLib, LicenseType}
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser
import java.net.InetAddress

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}
import java.util.Properties
import java.io.InputStream
import java.util.Calendar
import java.sql.Time

import akka.http.scaladsl.HttpsConnectionContext

/**
 * @author Pankaj on 9/1/15.
 *         debasish83: added getConfig API, updated handleWorkflow API for vertical tests
 *         sumanth: Converted ApplicationManager into a Driver program
 *                  Added scopt for parsing command line args
 *                  Refactored handleWorkflow API to runStreamHandler API
 *                  Added runBatchHandler API
 *         Hutashan: Added changes for batch and return code
 */

object ApplicationManager {
  val logger = LoggerFactory.getLogger(this.getClass)
  private var ssc: StreamingContext = null
  var sqlContext: SQLContext = null
  private var appConfig: ApplicationConfig = _
  private val threadLocalWorkflowConfig = new ThreadLocal[WorkflowConfig]();
  var stopStreaming: Boolean = false
  val ERROR_EXIT_CODE = -1
  private var embeddedHttpServer: EmbeddedHttpServer = _
  private var embeddedServer: EmbeddedServer = _
  private var uid = ""

  private var validLicense: Boolean = true
  // License check period in seconds. Default 60 seconds
  private var licenseCheckPeriod: Integer = 60
  private var licenseCheckTimeout: Calendar = Calendar.getInstance()

  def getEmbeddedServer: EmbeddedHttpServer = {
    embeddedHttpServer
  }

  def getEmbeddedServerV2: EmbeddedServer = {
    embeddedServer
  }

  /**
   *
   * @param configDir input config directory
   * @return an instance of ApplicationConfig
   */
  def getConfig(configDir: String = null, uid: String = null ): ApplicationConfig = {
    if (appConfig == null) {
      logger.info(s"Application is null.")
      if (uid ==null) {
        appConfig = new ApplicationConfig(ApplicationUtils.env, configDir, "")
      } else {
        appConfig = new ApplicationConfig(ApplicationUtils.env, configDir, uid)
      }
    }
    appConfig
  }

  def setWorkflowConfig(workflow: String): WorkflowConfig = {
    val workflowConfig = new WorkflowConfig(workflow)
    threadLocalWorkflowConfig.set(workflowConfig)
    workflowConfig
  }

  /**
   *
   * @return
   */
  def getWorkflowConfig(): WorkflowConfig = {

    threadLocalWorkflowConfig.get
  }

  // Case class to hold the command line arguments
  private case class Params(configDir: String = null,
                            workFlowName: String = null,
                            uid: String = null,
                            chaosMonkey: Boolean = false)

  // ApplicationManager entry point
  def main(args: Array[String]) {

   try {
     val defaultParams = Params()

     val parser = new OptionParser[Params]("ApplicationManager") {
       head("ApplicationManager: BDA Spark Application runner")
       opt[String]("config")
         .text(s"local config directory path")
         .optional
         .action((x, c) => c.copy(configDir = x))
       opt[String]("workflow")
         .text(s"workflow to run")
         .required
         .action((x, c) => c.copy(workFlowName = x))
       opt[String]("uid")
         .text(s"workflow to run")
         .optional
         .action((x, c) => c.copy(uid = x))
     }

     parser.parse(args, defaultParams).map { params =>
       run(params)
     } getOrElse {
       logger.error("Insufficient number of arguments {}", getUsage)
       System.exit(ERROR_EXIT_CODE)
     }
   } catch {
     case ex: Throwable => {
       logger.error(s"Exiting job because of following exception" ,
         ex)
       System.exit(ERROR_EXIT_CODE)
     }

   }
  }

  private def run(params: Params): Unit = {
    val workFlowToRun = params.workFlowName
    val uid = params.uid
    getConfig(params.configDir , uid)

    // load start up class
    initialize(appConfig)

    // Initialize license library and licenseValidationTimeout
    if (ApplicationManager.getConfig().env != "local" ) {
      LicenseLib.init(appConfig.zookeeperList)
    }

    val workflowConfig: WorkflowConfig = setWorkflowConfig(workFlowToRun)
    val runMode = workflowConfig.runMode
    val currentWorkflowName = ApplicationManager.getWorkflowConfig.workflow
    if (!runMode.equals("APIV2")) registerDriverHost(currentWorkflowName)
    runMode match {
      case "STREAM" => {

        initStreamThread (workFlowToRun)

      }
      case "BATCH" => {

        val dataSource = workflowConfig.dataSource

        dataSource match {

          case "KAFKA" => {

            initStreamThread(workFlowToRun)
          }
          case _ => {
            var sparkSession: SparkSession = null
            runBatchWorkFlow(workflowConfig, appConfig)(sparkSession)
            // if spark context is not stopped, stop it
            if (sparkSession != null && !sparkSession.sparkContext.isStopped) {
              sparkSession.stop
            }
          }
        }
      }
      case "API" => {
        val sc = new SparkContext(getSparkConf (appConfig))
        setHadoopConf(sc, appConfig)
        startHttpServer(sc, workflowConfig)

      }
      case "APIV2" => {
        startHttpServer(workflowConfig)
      }
      case _ => logger.error("Not implemented run mode. Exiting.. {}", runMode)
    }
  }

  private[framework] def initStreamThread (streamWorkflowName: String): Unit = {

    while (!stopStreaming) {
      val workflowth = new StreamWorkflowThread (streamWorkflowName)

      workflowth.start()

      this.synchronized {
        logger.info(s"Waiting to receive notification to restart streaming context")
        this.wait()

        if( stopStreaming ) {

          logger.info(s"Received notification to stop streaming context")
        } else {

          logger.info(s"Received notification to restart streaming context")
        }
        // Stop current workflow thread before re-creating a new workflow thread
        workflowth.interrupt

      }

      if( ssc != null && ssc.getState() != StreamingContextState.STOPPED) {

        logger.info(s"Do we need to stop spark context? ${stopStreaming}")

        try {

          // Stop the current streaming context
          ssc.stop(stopStreaming, false)


        } catch {
          case ex: Throwable => {

            logger.error(s"Consumed following exception because " +
              s"spark context was NOT stopped gracefully. {}" , ex.getMessage)
            throw ex
          }

        }
      }

      ssc = null

    }

  }

  private[framework] def initStreamWorkflow: Unit = {

    logger.info("Starting Streaming WorkFlow")

    val workflowConfig = ApplicationManager.getWorkflowConfig
    var dStreams: MMap[String, DStream[Row]] = null
    val dataSource = workflowConfig.dataSource
    logger.info(s"Running in stream mode with $dataSource datasource")

    val sparkConf: SparkConf = getSparkConf(appConfig)
    val runMode = workflowConfig.runMode

    dataSource match {
      case "HDFS" => {
        val minRememberDuration =
          ApplicationUtils.calculateHdfsRememberDuration

        val hdfsStreamConfig = workflowConfig.hdfsStream.asInstanceOf[Config]
        val checkPointDirectory = hdfsStreamConfig.getString("checkpointDirectory")

        logger.info(s"Go back by $minRememberDuration secs")
        // sparkConf.set("spark.streaming.fileStream.minRememberDuration",
        // s"${minRememberDuration}")
        sparkConf.set("spark.streaming.minRememberDuration", s"${minRememberDuration}s")

        ssc = HdfsDStream.createStreamingContext(hdfsStreamConfig.getString("batchTime").toInt,
          checkPointDirectory,
          sparkConf)
        setHadoopConf(ssc.sparkContext, appConfig)
        dStreams = HdfsDStream.createDStreams(ssc)
      }
      case "KAFKA" => {
        dStreams = initKafkaDstream(workflowConfig, sparkConf, runMode )
      }
      case _ => {
        logger.error("Mode not implemented. Exiting... {}", dataSource)
        System.exit(ERROR_EXIT_CODE)
      }
    }

    runStreamWorkFlow(dStreams)
    addStreamListeners(ssc, workflowConfig)
  }


  def initKafkaDstream(workflowConfig : WorkflowConfig, sparkConf : SparkConf,
                       runMode : String) : MMap[String, DStream[Row]] = {
    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]
    val streamsInfo = kafkaConfig.getConfigList("streamsInfo")
    val kafkaBrokerList = appConfig.kafkabrokerList

    logger.info("Kafka broker list faraz" + kafkaBrokerList)

    ssc = KafkaDStream.createStreamingContext(sparkConf)
    setHadoopConf(ssc.sparkContext, appConfig)
    val topicPartitionOffsets = MMap[TopicPartition, Long]()

    streamsInfo.asScala.foreach(streamInfo => {
      val topicName = streamInfo.getString("topicName")
      val partitionOffset = KafkaDStream.fetchPartitionOffsets(topicName, runMode, appConfig)
      topicPartitionOffsets ++= partitionOffset
    })

    val dStreams = KafkaDStream.createDStreams(
      ssc,
      kafkaBrokerList,
      kafkaConfig,
      topicPartitionOffsets.toMap,
      appConfig)
    dStreams
  }



  /**
   * method to create a SparkContext
   *
   * @return SparkContext object
   */
  private[framework] def getSparkConf(appConfig: ApplicationConfig): SparkConf = {

    val sparkConfigParam: Config = appConfig.sparkConfParam
    val sparkConf = new SparkConf
    sparkConf.setAppName(appConfig.appName)

    if (!sparkConfigParam.isEmpty) {
      val keyValueItr = sparkConfigParam.entrySet().iterator()
      while (keyValueItr.hasNext) {
        val sparkConfParam = keyValueItr.next()
        sparkConf.set(sparkConfParam.getKey, sparkConfParam.getValue.unwrapped().toString)
        logger.info(s"${sparkConfParam.getKey}: ${sparkConfParam.getValue.unwrapped().toString}")
      }
    }
    if (appConfig.env == "local") sparkConf.setMaster("local[5]")

    sparkConf
  }

  private[framework] def setHadoopConf(sc: SparkContext, appConfig: ApplicationConfig) = {

    val hadoopConfParameters: Config = appConfig.hadoopConfParam

    if (!hadoopConfParameters.isEmpty) {
      val keyValueItr = hadoopConfParameters.entrySet().iterator()
      while (keyValueItr.hasNext) {
        val hConf = keyValueItr.next()
        sc.hadoopConfiguration.set(hConf.getKey, hConf.getValue.unwrapped().toString)
        logger.info(s"${hConf.getKey}: ${hConf.getValue.unwrapped().toString}")
      }
    }
  }

  private[framework] def initialize(appConfig: ApplicationConfig): Unit = {
    // load start up class
    val applicationStartupClass: String = appConfig.applicationStartupClass
    val startupClass = ApplicationUtils.getStartupClass(applicationStartupClass)
    startupClass.init(appConfig.env, appConfig.configDir, appConfig.persistSchema)
  }

  /**
    * Start HTTP (Jetty) server
    *
    * @param workflowConfig
    */
  private[framework] def startHttpServer(sc: SparkContext, workflowConfig: WorkflowConfig): Unit = {

    val serverConfig = workflowConfig.httpServer
    if (serverConfig != null) {
      val provider = serverConfig.getString("provider")
      embeddedHttpServer = provider match {
        case "akka" => new AkkaHttpServer(sc)
        case "jetty" => new JettyHttpServer(sc, serverConfig)
      }
      logger.info(s"Starting $provider based Embedded HTTP Server")
      embeddedHttpServer.init(serverConfig)
      embeddedHttpServer.start(serverConfig)
    }
  }

  /**
   * Start HTTP (Jetty) server
   *
   * @param workflowConfig
   */
  private[framework] def startHttpServer(workflowConfig: WorkflowConfig): Unit = {

    val serverConfig = workflowConfig.httpServer
    if (serverConfig != null) {
      val provider = serverConfig.getString("provider")
      embeddedServer = provider match {
        case "akka" => {
          val enableHttps = serverConfig.getBoolean("enableHttps")
          if (!enableHttps) {
            new AkkaServer
          } else {
            val https: HttpsConnectionContext = HttpsConnectionContextBuilder.build(serverConfig)
            new AkkaTlsServer(httpsContext = https)
          }
        }
        case "jetty" => new JettyServer(serverConfig)
      }
      logger.info(s"Starting $provider based Embedded HTTP Server")
      embeddedServer.init(serverConfig)
      embeddedServer.start(serverConfig)
    }
  }

  /**
   * Method to print the usage of this Application
   *
   * @return
   */
  private def getUsage: String = {
    "ApplicationManager --config <configDir> --workflow <workFlow>"
  }

  /**
   * Public API for running Stream workflow
   *
   * @param dStreams input dstreams
   */
  def runStreamWorkFlow(dStreams: MMap[String, DStream[Row]]) : Unit = {
    StreamingHandler.handleWorkflow(dStreams)
  }

  /**
   * Public API for adding listener to StreamingContext
   *
   * @param ssc StreamingContext
   * @param workflowConfig ApplicationConfig object
   */
  def addStreamListeners(ssc: StreamingContext,
                         workflowConfig: WorkflowConfig) : Unit = {

    startHttpServer(ssc.sparkContext, workflowConfig)

    StreamingHandler.addStreamListener(ssc, workflowConfig)
  }

    def validateLicense(): Unit = {
      if (ApplicationManager.getConfig().env == "local" ) {
        return
      }

      // Check license for validity only if current date is higher than or
      // equal to the timestamp of last license validity check.
      var c = Calendar.getInstance();
      if (0 <= c.getTime().compareTo(licenseCheckTimeout.getTime())) {
        licenseCheckTimeout = Calendar.getInstance()
        licenseCheckTimeout.add(Calendar.SECOND, licenseCheckPeriod);
        validLicense = LicenseLib.isValid(LicenseType.PLATFORM)
      }

      if (validLicense == false) {
        throw new LicenseException("PLATFORM license is not valid")
      }
    }

  def runBatchWorkFlow(workFlow: WorkflowConfig,
                       appConfig: ApplicationConfig,
                       maxIters: Long = -1)
                      (implicit sparkSession: SparkSession): Unit = {
    BatchHandler.scheduleBatchRun(workFlow, appConfig, maxIters, sparkSession)
  }

  def getSynchronizationTime: String = {

    val workflowConfig = ApplicationManager.getWorkflowConfig
    val synchronizationTime = ApplicationUtils.getSyncWorkflowWorkflowTime(
      appConfig, workflowConfig)

    synchronizationTime match {

      case Some(value) => {

        value.toString
      }
      case None => null

    }
  }

  def updateWorkflowTime(timeStamp: Long, workflowName: String = ""): Unit = {

    val currentWorkflowName =
      if (workflowName.size == 0) {
        ApplicationManager.getWorkflowConfig.workflow
      } else {
        workflowName
      }

    ApplicationUtils.updateCurrentWorkflowTime(
      currentWorkflowName,
      timeStamp,
      appConfig.zookeeperList)
  }

  def updateSynchronizationTime(timeStamp: Long, workflowName: String = ""): Unit = {

    val currentWorkflowName =
      if ( workflowName.size == 0) {
        ApplicationManager.getWorkflowConfig.workflow
      } else {
        workflowName
      }

    ApplicationUtils.updateSynchronizationTime(
      currentWorkflowName,
      timeStamp,
      appConfig.zookeeperList)
  }


   def registerDriverHost(currentWorkflowName : String) : Unit = {
    val hostName = {
      try {
        InetAddress.getLocalHost().getHostName()
      } catch {
        case e: Exception =>
          "NA"
      }
    }
     logger.info("Registering hostName:" + hostName)
     ApplicationUtils.registerHostName(
      currentWorkflowName,
       hostName,
      appConfig.zookeeperList)
  }


  /**
   * method to read the kafka config params if any and return a Map[String, Object]
   * @param appConf the ApplicationConfig instance
   * @return a Map[String, Object]
   */
  def getKafkaConf(appConf: ApplicationConfig = null): Map[String, Object] = {
    val kafkaConfigParam: Config = if ( appConf != null ) {
      appConf.kafkaConfParam
    }
    else {
      appConfig.kafkaConfParam
    }

    val tempConf = scala.collection.mutable.Map[String, String]()

    if (!kafkaConfigParam.isEmpty) {
      val keyValueItr = kafkaConfigParam.entrySet().iterator()
      while (keyValueItr.hasNext) {
        val confParam = keyValueItr.next()
        tempConf += (confParam.getKey -> confParam.getValue.unwrapped().toString)

        logger.info(s"${confParam.getKey}: ${confParam.getValue.unwrapped().toString}")
      }
    }
    // for local as well as jenkins build
    // should use available port for Kafka rather than defined in conf
    if (ApplicationManager.getConfig().env == "local" ) {

      tempConf += ("bootstrap.servers" -> KafkaApplicationUtils.kafkaBrokers)
    }
    tempConf.toMap[String, Object]
  }
}

class StreamWorkflowThread (streamWorkflowName: String) extends Thread {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def run() : Unit = {
    logger.info(s"Starting a new workflow")

    try {
      ApplicationManager.setWorkflowConfig(streamWorkflowName)
      ApplicationManager.initStreamWorkflow

    } catch {

      case ex: InterruptedException => {

        logger.info("Thread interrupted to start another thread.")
      }
      case ex: Throwable => {

        ex.printStackTrace()
        logger.error("Stopping job", ex.getMessage)
        ApplicationManager.stopStreaming = true
      }
    }

    ApplicationManager.synchronized {
      ApplicationManager.notifyAll()

    }
  }
}
