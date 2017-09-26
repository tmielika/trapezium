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
package com.verizon.bda.trapezium.framework.handler

import java.sql.Time
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Timer, TimerTask}
import com.typesafe.config.{Config, ConfigList, ConfigObject}
import com.verizon.bda.trapezium.framework.kafka.KafkaSink
import com.verizon.bda.trapezium.framework.manager.{WorkflowConfig, ApplicationConfig}
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import com.verizon.bda.trapezium.framework.{ApplicationManager, BatchTransaction}
import com.verizon.bda.trapezium.validation.DataValidator
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDateTime
import org.slf4j.LoggerFactory
import scala.StringBuilder
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}
import com.verizon.bda.trapezium.framework.utils.Waiter
import org.slf4j.LoggerFactory

/**
 * @author sumanth.venkatasubbaiah Handles the batch workflows
 *         debasish83: integrated timer scheduling API for local and integration test
  *        hutashan: Added accumlator for batch stats
 *
 */
private[framework] class BatchHandler(val workFlowConfig : WorkflowConfig,
                                      val appConfig: ApplicationConfig,
                                      val maxIter: Long)
                                     (implicit var sc: SparkContext) extends TimerTask with Waiter {
  /**
   * method called by the TimerTask when a run is scheduled.
   */
  val simpleFormat = new SimpleDateFormat("yyyy-MM-dd")
  val logger = LoggerFactory.getLogger(this.getClass)
  override def run(): Unit = {
    val startTime = System.currentTimeMillis()
    var endTime : Long = System.currentTimeMillis() // Overriding
    try {
      logger.info("workflow name" + workFlowConfig.workflow)

      // Check if license is valid
      ApplicationManager.validateLicense()

      ApplicationManager.setWorkflowConfig(workFlowConfig.workflow)
      val batchMode = workFlowConfig.dataSource

      logger.info("mode == " + batchMode )
      batchMode match {
        case "HDFS" => {
          logger.info(s"Running BatchHandler in mode: $batchMode")
          handleWorkFlow
          stopContext
          endTime = System.currentTimeMillis()
          printLogger(startTime, endTime)
          logger.info(s"batch completed for this run.")
          }
          case _ => {
            logger.error("Not implemented mode. Exiting.. ", batchMode)
            notifyStop()
          }
        }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        logger.error(s"Error in running the workflow", e.getMessage)
        endTime = System.currentTimeMillis()
        printLoggerError(startTime, endTime)
        notifyError(e)
    }
  }


  def printLogger(startTime : Long, endTime : Long) : Unit = {

    logger.info(s" Job Summary Status : Passed" )
    val hdfsBatchConfig = workFlowConfig.hdfsFileBatch.asInstanceOf[Config]
    val batchInfoList = hdfsBatchConfig.getList("batchInfo")
    var inputNamePath : mutable.StringBuilder = new StringBuilder("[")
    batchInfoList.asScala.foreach { batchConfig =>
      val batchData: Config = batchConfig.asInstanceOf[ConfigObject].toConfig
      val dataDir = FileSourceGenerator.getDataDir(appConfig, batchData)
      inputNamePath.append(s"{path: $dataDir}")
    }


    val diff = endTime - startTime
    val diffSeconds = diff / 1000 % 60;
    val diffMinutes = diff / (60 * 1000) % 60;
    val diffhours = diff / (60 * 60 * 1000) % 60;
    logger.info(s"Run complete for appname=${workFlowConfig.workflow}" +
      s",job_date=${new Date(System.currentTimeMillis())}" +
      s",status=Passed" +
      s",starttime=${new Date(startTime)}" +
      s",endtime=${new Date(endTime)}" +
      s",duration=$diffhours:$diffMinutes:$diffSeconds" +
      s",input=$inputNamePath")

  }


  def printLoggerError(startTime : Long, endTime : Long) : Unit = {
    val diff = endTime - startTime
    val diffSeconds = diff / 1000 % 60;
    val diffMinutes = diff / (60 * 1000) % 60;
    val diffhours = diff / (60 * 60 * 1000) % 60;
    logger.info(s"Run complete for appname=${workFlowConfig.workflow}" +
      s",job_date=${new Date(System.currentTimeMillis())}" +
      s",status=failed" +
      s",starttime=${new Date(startTime)}" +
      s",endtime=${new Date(endTime)}" +
      s",duration=$diffhours:$diffMinutes:$diffSeconds")

  }


  def createContext : SparkContext = {
    logger.info("getting context")
    if (sc == null || sc.isStopped) {
      logger.info("Starting context")
      sc = new SparkContext(ApplicationManager.getSparkConf(appConfig))
      ApplicationManager.setHadoopConf(sc, appConfig)
      logger.info("Started context")
     }
    sc
 }

  def stopContext: Unit = {

    if (ApplicationUtils.env != "local" && !sc.isStopped &&
      (workFlowConfig.httpServer == null || !workFlowConfig.httpServer.hasPath("hostname"))) {
      logger.info("Stoping context")
      sc.stop
      logger.info("Stopped context")
    }
  }

  def handleWorkFlow : Unit = {

    var runCount = 0
    do {
      ApplicationUtils.waitForDependentWorkflow(appConfig, workFlowConfig)
      createContext
      ApplicationManager.startHttpServer(sc, workFlowConfig)

      val (workflowTimeToSave, runSuccess, mode) = executeWorkFlow
      logger.info("going to call complete method")

      completed(workflowTimeToSave, runSuccess, mode)

      if( mode.equalsIgnoreCase("groupFile")) {
        val keyFileProcessed = s"/etl/${workFlowConfig.workflow}/fileProccessed"
        logger.info("Deleting key fileProccessed")
        ApplicationUtils.deleteKeyZk(keyFileProcessed, appConfig.zookeeperList)
      }

      runCount = runCount + 1
    } while (runCount < maxIter)

    logger.info("Could not generate RDD in this batch run.")
    if ((workFlowConfig.singleRun) || ( !(runCount < maxIter) && (maxIter != -1))) {
      notifyStop()
    }
  }


  def executeWorkFlow() : (Time, Boolean, String) = {
    var runSuccess = false
    var mode: String = "None"

    val fileSourceGenerator = new FileSourceGenerator(workFlowConfig, appConfig, sc).get

    var workflowTimeToSave = new Time(System.currentTimeMillis)


    fileSourceGenerator.foreach( fileSource => {

      workflowTimeToSave = fileSource._1
      val rddMap = fileSource._2._1
      if (fileSource._2._2.toString.equalsIgnoreCase("eventType")) {
        logger.info("workflowTimeToSave " + workflowTimeToSave + "rddMap " +
          rddMap.keySet.toString()  )
        if (rddMap.size > 0) {
          runSuccess = processAndPersist(workflowTimeToSave, rddMap)
        }
        val zkpath = ApplicationUtils.
          getCurrentWorkflowKafkaPath(appConfig, workFlowConfig.workflow, workFlowConfig)
          ApplicationUtils.updateZkValue(zkpath + "/0",
            fileSource._2._3.toString, appConfig.zookeeperList )
        logger.info("update zk " +zkpath + "/0" + " = " + fileSource._2._3.toString  )

      } else { if (fileSource._2._2.toString.equalsIgnoreCase("groupFile") ) {
        mode = fileSource._2.toString
        val LastCal = Calendar.getInstance()
        val currentCal = Calendar.getInstance()
        val keyFileProcessed = s"/etl/${workFlowConfig.workflow}/fileProccessed"
        val lastProcessedDateFiles = ApplicationUtils.getValFromZk(keyFileProcessed,
          appConfig.zookeeperList)
        if (lastProcessedDateFiles != null) {
          val lastProcessedDateFilesStr = new String(lastProcessedDateFiles).toLong
          logger.info("Test processed time " + new Date(lastProcessedDateFilesStr))
          LastCal.setTime(new Date(lastProcessedDateFilesStr))
          currentCal.setTime(workflowTimeToSave)
          if (LastCal.before(currentCal)) {
            logger.info(s"Starting Batch transaction with workflowTime: $workflowTimeToSave")
            if (rddMap.size > 0) {
              runSuccess = processAndPersist(workflowTimeToSave, rddMap)
            }
            else logger.warn("No new RDD in this batch run.")
          } else {
            logger.debug("Skipping files")
          }
        } else {
          logger.info(s"Starting Batch transaction with workflowTime: $workflowTimeToSave")
          logger.info(s"file split mode first time")
          if (rddMap.size > 0) {
            runSuccess = processAndPersist(workflowTimeToSave, rddMap)
          }
          else logger.warn("No new RDD in this batch run.")
        }
        val processedTime: Long = workflowTimeToSave.getTime
        logger.info("setting date " + new Date(processedTime.toLong))
        ApplicationUtils.updateZookeeperValue(keyFileProcessed,
          workflowTimeToSave.getTime, appConfig.zookeeperList)
      } else {
        if (rddMap.size > 0) {
          logger.info("files from batch time")
          runSuccess = processAndPersist(workflowTimeToSave, rddMap)
        }
        else logger.warn("No new RDD in this batch run.")
      }
    }
      //
    })

    (workflowTimeToSave, runSuccess, mode)
  }



  def processAndPersist(workflowTime: Time,
                        dfMap: MMap[String,
                          DataFrame]): Boolean = {
    try {
      val processedDataMap = processTransactions(workflowTime, dfMap)
      saveData(workflowTime, processedDataMap )
    } catch {
      case e: Throwable => {
        logger.error(s"Caught Exception during processing", e.getCause)
        rollBackTransactions(workflowTime)
       throw e

      }
    }
  }

  /**
   * method used to call the process method of the transactions in the current workflow
   * @param dataMap
   * @return map of string and DataFrame
   */
  private def processTransactions(workflowTime: Time,
                                  dataMap: MMap[String, DataFrame]): MMap[String, DataFrame] = {
    val transactionList = workFlowConfig.transactions
    var allTransactionInputData = MMap[String, DataFrame]()
    transactionList.asScala.foreach { transaction: Config => {

      var transactionInputData = MMap[String, DataFrame]()
      val transactionClassName = transaction.getString("transactionName")
      val transactionClass: BatchTransaction = ApplicationUtils.getWorkflowClass(
        transactionClassName, appConfig.tempDir).asInstanceOf[BatchTransaction]

      transactionClass.preprocess(sc)

      // MMap to hold the list of all input data sets for this transaction
      val inputData: ConfigList = transaction.getList("inputData")
      inputData.asScala.foreach { s =>
        val inputSource = s.asInstanceOf[ConfigObject].toConfig
        val inputName = inputSource.getString("name")

        if (dataMap.keys.filter(name => name.contains(inputName)).size > 0) {
          dataMap.keys.filter(name => name.contains(inputName)).map(name => {
            logger.info(s"Adding RDD ${name} to transaction " +
              s"workflow ${transactionClassName}")
            transactionInputData += ((name, dataMap(name)))
          }
          )
        } else if (allTransactionInputData.contains(inputName)) {
          // DF is output from an earlier transaction
          logger.info(s"Adding RDD ${inputName} to transaction " +
            s"workflow ${transactionClassName}")
          transactionInputData += ((inputName, allTransactionInputData(inputName)))
        }
      }

      logger.info(s"Calling $transactionClass.process with " +
        s"input $transactionInputData")
      val processedData = transactionClass.processBatch(transactionInputData.toMap, workflowTime)

      val persistData = transaction.getString("persistDataName")

      if (transactionInputData.contains(persistData)) {

        logger.error("Config file is wrong",
          s"${persistData} returned from $transaction " +
            s"already present in ${dataMap.toString}")
        throw new Exception
      } else {
        transactionInputData += ((persistData, processedData))
      }

      allTransactionInputData ++= transactionInputData
    } // end list of transactions
      logger.info("Overall RDD collection " + dataMap)
    }

    allTransactionInputData
  }

  /**
    * method to update timestamp in zookeeper
    * This method is called only if all the transactions that are part of the workflow
    * are complted successfully.
    */
  private def completed(workflowTime: Time, isSuccess: Boolean, mode : String ): Unit = {
    try {
      if (isSuccess) {
        logger.info(s"Workflow with time $workflowTime completed successfully.")
        ApplicationManager.updateWorkflowTime( workflowTime.getTime, workFlowConfig.workflow)
      } else {
        logger.info("process method is returning false")
      }
    } catch {
      case e: Throwable => {
        logger.error(s"Caught Exception during processing {}", e.getMessage)
        rollBackTransactions(workflowTime)
        throw e
      }
    }

  }

  /**
   * method to roll back all the transactions
   * All the transactions in the workflow will be rolled back if any error/exception occurs
   * during the work-flow execution.
   */
  private def rollBackTransactions(workflowTime: Time): Unit = {

    logger.warn(s"Rolling back all transactions in the current workFlow")

    val transactionList = workFlowConfig.transactions
    transactionList.asScala.foreach { transaction: Config =>
      val transactionClassName = transaction.getString("transactionName")
      val transactionClass: BatchTransaction =
        ApplicationUtils.getWorkflowClass(
          transactionClassName,
          appConfig.tempDir).asInstanceOf[BatchTransaction]
      try {
        logger.info(s"Rolling back transaction: $transactionClassName with batchTime:" +
          s" ${workflowTime}")
        transactionClass.rollbackBatch(workflowTime)
      } catch {
        case e: Throwable =>
          logger.error(s"Exception caught while rollback of " +
            s"transaction $transactionClassName with batchTime: ${workflowTime} {}"
            , e.getMessage)
          throw e
      }
    }
  }

  /**
   *
   * @param dataMap map of DataFrames which contains the dataframes that will be persisted.
   * @return true if persist is successful, false otherwise
   */
  private def saveData(workflowTime: Time,
                       dataMap: MMap[String, DataFrame]): Boolean = {
    val transactionList = workFlowConfig.transactions
    transactionList.asScala.foreach { transaction: Config => {
      val transactionClassName = transaction.getString("transactionName")
      val transactionClass: BatchTransaction = ApplicationUtils.getWorkflowClass(
        transactionClassName, appConfig.tempDir).asInstanceOf[BatchTransaction]

      val persistDataName = transaction.getString("persistDataName")
      val isPersist = {
        if (transaction.hasPath("isPersist")){
          transaction.getBoolean("isPersist")
        } else {
          true
        }
      }

      if (dataMap.contains(persistDataName) && isPersist) {
        try {
          val persistDF = dataMap(persistDataName)
            logger.info(s"Persisting data: ${persistDataName}")
          val callbackEvent = transactionClass.persistBatch(persistDF, workflowTime)
          if (callbackEvent!=null && !(callbackEvent.isEmpty)){
            val returnEvent = callbackEvent.get
            logger.info("Inside return event")
            val kafkaConf = ApplicationManager.getKafkaConf()
            val kafkaSink = sc.broadcast(KafkaSink(kafkaConf))
            logger.info("is spark context live " + sc.isStopped)
            logger.info("topic " + returnEvent + " sending to topic "
              + returnEvent.mkString(","))
            for (event <- returnEvent) {
              kafkaSink.value.send(workFlowConfig.workflow, event.toString())
              logger.info("Kafka message sent" + event.toString())
            }

          }
          DataValidator.printStats()
        } catch {
          case e: Throwable => {
            logger.error(s"Exception occured while running transaction: $transactionClassName",
              e.getMessage )
            throw e
          }
        }
      } // end if
    }
    } // end transaction list iteration
    true
  } // end saveData
}

private[framework] object BatchHandler {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   *
   * Schedules the batch workflows
   * @param workFlowConfig the workflow config
   * @param appConfig the ApplicationConfig object
   * @param sc SparkContext instance
   */
  def scheduleBatchRun(workFlowConfig: WorkflowConfig,
                       appConfig: ApplicationConfig,
                       maxIters: Long,
                       sc: SparkContext): Unit = {
    val timer: Timer = new Timer(true)
    try {
      logger.info("Starting Batch WorkFlow")
      val batchTime: Long =
        workFlowConfig
          .hdfsFileBatch
          .asInstanceOf[Config]
          .getLong("batchTime")

      if (batchTime <= 0) {
        logger.error("Invalid batchTime.", "batchTime has to be greater than 0 seconds")
        System.exit(-1)
      }

      val batchHandler = new BatchHandler(workFlowConfig, appConfig, maxIters)(sc)
      val nextRun = workFlowConfig.hdfsFileBatch.asInstanceOf[Config].getString("timerStartDelay")
      val nextRunHourMinute = nextRun.split(":")
      if (nextRunHourMinute.size == 1) {
        timer.scheduleAtFixedRate(batchHandler, nextRunHourMinute(0).toInt, batchTime * 1000)
      } else {
        timer.scheduleAtFixedRate(batchHandler, getdtNextRun(nextRunHourMinute), batchTime * 1000)
      }
      if(batchHandler.waitForStopOrError) stopTimer(timer)
    } catch {
      case e: Throwable =>
        logger.error("Caught exception during scheduling BatchJob", e.getCause)
        stopTimer(timer)
        throw e
    }
  }

  def getdtNextRun( nextRunHourMinute: Array[String]) : java.util.Date = {
    val calendar = Calendar.getInstance()
    calendar.setTime(new java.util.Date())
    if (nextRunHourMinute.size > 1) {
      val currentHour = calendar.get(Calendar.HOUR_OF_DAY)
      val currentMin = calendar.get(Calendar.MINUTE)
      val scheduleHour = nextRunHourMinute(0).toInt
      val scheduleMin = nextRunHourMinute(1).toInt
      if (currentHour > scheduleHour || (currentHour == scheduleHour && currentMin >scheduleMin)){
        calendar.set(Calendar.HOUR_OF_DAY, (nextRunHourMinute(0).toInt + 24))
      } else {
        calendar.set(Calendar.HOUR_OF_DAY, nextRunHourMinute(0).toInt)
      }
      calendar.set(Calendar.MINUTE, nextRunHourMinute(1).toInt)
      calendar.set(Calendar.SECOND, 0)
    }
    if (nextRunHourMinute.size > 2) {
      calendar.set(Calendar.SECOND, nextRunHourMinute(2).toInt)
    }
     val dt = calendar.getTime()
     logger.info(s"Job scheduled for $dt")

    return dt
  }

  def stopTimer(timer: Timer): Unit = {
    timer.cancel()
    timer.purge()
  }
}
