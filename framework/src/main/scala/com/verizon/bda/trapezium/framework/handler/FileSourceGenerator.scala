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
import java.{lang, util}
import java.util.Date
import java.util.regex.Pattern

import com.typesafe.config.{Config, ConfigObject}
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.kafka.{KafkaDStream, KafkaRDD}
import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig, WorkflowConfig}
import com.verizon.bda.trapezium.framework.utils.{ApplicationUtils, ScanFS}
import com.verizon.bda.trapezium.transformation.DataTranformer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.json.JSONObject
import org.slf4j.LoggerFactory

import scala.collection.mutable.StringBuilder
import scala.collection.mutable.{Map => MMap}
import scala.collection.JavaConverters.asScalaBufferConverter
// scalastyle:off
import scala.collection.JavaConversions._
// scalastyle:on

/**
  *
  * @param workflowConfig the workflow for which RDD's needs to be created
  * @param appConfig Application config
  * @param sparkSession Spark Session instance
  */
// TO DO :
private[framework] case class
FileSourceGenerator(workflowConfig: WorkflowConfig,
                    appConfig: ApplicationConfig,
                    sparkSession: SparkSession)  // readFullDataset is true then run only 1 times
  extends SourceGenerator(workflowConfig, appConfig, sparkSession.sparkContext) {

  import FileSourceGenerator.getSortedFileMap
  import FileSourceGenerator.getWorkFlowTime

  var mapFile: java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]] = null
  var mapFileAllFile: java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]] = null

  import FileSourceGenerator.getElligbleFiles
  import FileSourceGenerator.getDataDir
  import FileSourceGenerator.getCurrentWorkflowTime

  def getDFFromKafka (topicName : String) : util.TreeMap[Time,
    (MMap[String, DataFrame], String, Long)] = {
    val dataSources = new java.util.TreeMap[Time, (MMap[String, DataFrame], String, Long)]
    val rddOption = KafkaRDD.getRDDFromKafka(topicName, appConfig, workflowConfig, sparkSession.sparkContext)
    if (rddOption.isDefined) {
      logger.info("inside rddOption.isDefined")

      val rdd = rddOption.get
      val collectRDD = rdd._1.map(record => record.value())

      var counter = rdd._2
      logger.info("collectRDD " + collectRDD.toString + "int counter " + counter)
      collectRDD.foreach(row => {
        val dfMap = FileSourceGenerator.getDFFromStream(row, sparkSession.sparkContext)
        val workflowTime = new Time(System.currentTimeMillis())
        logger.info("Kafka --> on offset" + rdd._2)
        counter = counter + 1
        dataSources.put(workflowTime, (dfMap, "eventType", counter))

      })
    } else {
      logger.info("Kafka --> Data source is empty")
    }
    dataSources
  }




  def get(): Seq[(Time, (MMap[String, DataFrame], String, Long))] = {
    var dataSources = new java.util.TreeMap[Time,
      (MMap[String, DataFrame], String, Long)]
    val dataSourcesNoSplit = MMap[String, DataFrame]()
    var workflowTime = new Time(System.currentTimeMillis)
    batchInfoList.asScala.foreach { batchConfig =>
      val batchData: Config = batchConfig.asInstanceOf[ConfigObject].toConfig
      val name = batchData.getString("name")
      val dtDirectory = batchData.getConfig("dataDirectory")
      if ((dtDirectory.hasPath("sourcetype") &&
        (dtDirectory.getString("sourcetype")).equals("event"))) {  // null check
        val workSpaceTopic = batchData.getConfig(
          "dataDirectory").getString(ApplicationUtils.env)
        dataSources = getDFFromKafka(workSpaceTopic)
      } else {
        val dataDir = getDataDir(appConfig, batchData)
        val currentWorkflowTime = getCurrentWorkflowTime(appConfig,
          workflowConfig, batchData)
        if (batchData.hasPath("groupFile")) {
          mode = "groupFile"
          val groupFileConf = batchData.getConfig("groupFile")
          val workSpaceDataDirectory = batchData.getConfig(
            "dataDirectory").getString(ApplicationUtils.env)
          val batchFiles = ScanFS.getFiles(dataDir, 0)
          logger.info(s"Number of available files for processing for source $name = " +
            s": ${batchFiles.size}")
          if (batchFiles.size > 0) {
            if (mapFileAllFile == null) {
              mapFileAllFile = new java.util.TreeMap[Date,
                java.util.HashMap[String, StringBuilder]]()
            }
            mapFileAllFile = getSortedFileMap(name,
              batchFiles.toList, groupFileConf,
              workSpaceDataDirectory, mapFileAllFile)
            if (groupFileConf.hasPath("offset")) {
              mapFile = getElligbleFiles(mapFileAllFile,
                workflowConfig, appConfig, groupFileConf)
            } else {
              mapFile = mapFileAllFile
            }
          }
          val mapFileIterator = mapFile.iterator
          while (mapFileIterator.hasNext) {
            val output = mapFileIterator.next
            val (sDate, sourceMap) = (output._1, output._2)
            val files = sourceMap(name)
            logger.info("source name " + name)
            logger.info("fileSplit running for date " + sDate + " , files are " +
              files.toString())
            workflowTime = getWorkFlowTime(sDate, currentWorkflowTime)
            val dataMap = addDF(sparkSession, files.toString().split(","), batchData)
            val existingDataMap = dataSources.get(workflowTime)
            if (existingDataMap != null) {
              logger.info(s"Entry exists for this workflow time ${existingDataMap}")
              existingDataMap._1 ++= dataMap
              dataSources.put(workflowTime, (existingDataMap._1, mode, 0))
            } else {
              dataSources.put(workflowTime, (dataMap, mode, 0))
            }
          }
          logger.info(s"dataSources size --> ${dataSources.size}")
        }
        else {
          val batchFiles = ScanFS.getFiles(dataDir, currentWorkflowTime)
          logger.info("source name " + name)
          logger.info(s"Number of available files for processing for source $name = " +
            s": ${batchFiles.size}")
          if (batchFiles.size > 0) {
            logger.info(s"list of files for this run " + batchFiles.mkString(","))
            val dataMap = addDF(sparkSession, batchFiles, batchData, dataDir)
            dataSourcesNoSplit ++= dataMap
          }
        }
        val iter = dataSources.iterator
        iter.foreach(dataSource => {
          // Add no split source to other sources
          dataSource._2._1 ++= dataSourcesNoSplit
        })
        if (dataSources.isEmpty) {
          dataSources.put(workflowTime, (dataSourcesNoSplit, mode, 0))
        }
      }
    }
 //   logger.info(s"dataSources --> ${dataSources}")
    dataSources.toMap.toList.sortBy(_._1.getTime)
  }


  import DynamicSchemaHelper.generateDataFrame
  def  addDF (sparkSession: SparkSession,
              input: Array[String],
              batchData: Config,
              dataDir: String): MMap[String, DataFrame] = synchronized {

    var dataMap = MMap[String, sql.DataFrame]()

    val name = batchData.getString("name")
    SourceGenerator.getFileFormat(batchData).toUpperCase match {
      case "PARQUET" => {
        logger.info(s"input source is Parquet")
        val df: DataFrame = sparkSession.read.option("basePath", dataDir).parquet(input: _*)
        dataMap += ((name, df))
      }
      case "AVRO" => {
        logger.info(s"input source is Avro")
        dataMap += ((name, sparkSession.read.format("com.databricks.spark.avro")
          .load(input: _*)))
      }
      case "JSON" => {
        logger.info(s"input source is Json")
        dataMap += ((name, sparkSession.read.text(input: _*)))
      }
      case _ => {
        val rdd = sparkSession.read.textFile(input.mkString(",")).rdd.map(line => Row(line.toString))
        dataMap += ((name, SourceGenerator.validateRDD(rdd, batchData)))
      }
    }
    if (batchData.hasPath("transformation")) {
      dataMap(name) = DataTranformer.transformDf(dataMap(name), batchData)
    }
    dataMap
  }
}

object FileSourceGenerator {
  val logger = LoggerFactory.getLogger(this.getClass)

  def getDFFromStream(json : String, sc: SparkContext) : MMap[String, DataFrame] = {
    val dataMap = MMap[String, DataFrame]()
    logger.info("json to read " + json)
    try {
      val jObject = new JSONObject(json)
      val jArray = jObject.getJSONArray("datasources")
      for( i <- 0 to jArray.length()-1){
        logger.info(s"input source is Parquet" )
        val jObject = jArray.get(i).asInstanceOf[JSONObject]
        val sourcesName = jObject.getString("name")
        val sourceLocation = jObject.getString("location")
        logger.info("source location : " + sourceLocation)
       try {
         dataMap += ((sourcesName, SQLContext.getOrCreate(sc).
           read.parquet(sourceLocation)))
       } catch {
         case ex : AssertionError => {
           logger.info("parquet file is not present on location AssertionError excpetion "
             + sourceLocation)

         }

       }
      }
    } catch {
      case ex : Exception => {
        logger.info("issue in json " + json)
        ex.printStackTrace()
      }

    }

    dataMap
  }





  def getElligbleFiles(fileMap: java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]],
                       workflowConfig: WorkflowConfig,
                       appConfig: ApplicationConfig,
                       offsetConfig: Config):
  java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]] = {
    val batchTime: Long =
      workflowConfig
        .hdfsFileBatch
        .asInstanceOf[Config]
        .getLong("batchTime")
    val startOffset: Long = offsetConfig.getLong("offset") * batchTime * 1000
    logger.info("startOffset :" + startOffset)
    val calculateFolderTimeStamp: Long = System.currentTimeMillis() + startOffset
    val endDate = new Date(calculateFolderTimeStamp)
    val startDate = new Date(ApplicationUtils.getCurrentWorkflowTime(appConfig, workflowConfig))
    logger.info("Date range is startDate :" + startDate + " enddate : " + endDate)
    // TO-DO may be better implementation
    val mapFile = new java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]]()
    fileMap.foreach(f => {
      val x = f._1
      val fileList = f._2
      if ((x.after(startDate)) && (x.equals(endDate) || x.before(endDate))) {
        mapFile.put(x, fileList)
      }
    })
    mapFile
  }

  /**
    * This method invoked only once in each cycle if filesplit is required.
    * Sorted files according to date
    */
  def getSortedFileMap(source: String,
                       inputFileList: java.util.List[String],
                       groupFileConf: Config,
                       workSpaceDataDirectory: String,
                       mapFile: java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]]
                      ): java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]] = {

    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat(
      groupFileConf.getString("dateformat"))
    if (!groupFileConf.hasPath("regexFile")) {
      val startDateIndex = groupFileConf.getString("startDateIndex").toInt
      val endDateIndex = groupFileConf.getString("endDateIndex").toInt
      val workSpaceLength = {
        if (inputFileList(0) != null) {
          inputFileList(0).indexOf(workSpaceDataDirectory) + workSpaceDataDirectory.length
        } else {
          logger.info("length count == 0")
          0
        }
      }
      inputFileList.foreach { file =>
        try {
          val fileLen = file.length
          val fileDate = file.substring(
            workSpaceLength + startDateIndex, workSpaceLength + endDateIndex)
          val dt: Date = simpleDateFormat.parse(fileDate)
          val sourceMap = mapFile.get(dt)
          if (sourceMap == null) {
            val hs = new java.util.HashMap[String, StringBuilder]()
            hs.put(source, new StringBuilder(file))
            mapFile.put(dt, hs)
          } else {
            val fileName = sourceMap.get(source)
            if (fileName == null) {
              sourceMap.put(source, new StringBuilder(file))
              mapFile.put(dt, sourceMap)
            } else {
              fileName.append(",")
              fileName.append(file)
              sourceMap.put(source, fileName)
              mapFile.put(dt, sourceMap)
            }

          }
        }
        catch {
          case e: Throwable =>
            logger.error(s"Error in file parsing. File name" +
              s" $file ", e.getCause)
        }
      }
    } else {
      val pattern = Pattern.compile(groupFileConf.getString("regexFile"))
      inputFileList.foreach { file =>
        try {
          val fileDate = {
            val matcher = pattern.matcher(file)
            if (matcher.find()) {
              matcher.group()
            } else ""
          }
          val dt: Date = simpleDateFormat.parse(fileDate)
          val sourceMap = mapFile.get(dt)
          if (sourceMap == null) {
            val hs = new java.util.HashMap[String, StringBuilder]()
            hs.put(source, new StringBuilder(file))
            mapFile.put(dt, hs)
          } else {
            val fileName = sourceMap.get(source)
            if (fileName == null) {
              sourceMap.put(source, new StringBuilder(file))
              mapFile.put(dt, sourceMap)
            } else {
              fileName.append(",")
              fileName.append(file)
              sourceMap.put(source, fileName)
              mapFile.put(dt, sourceMap)
            }

          }
        }
        catch {
          case e: Throwable =>
            logger.error(s"Error in file parsing. File name" +
              s" $file ", e.getCause)
        }
      }

    }

    mapFile
  }

  // TODO
  // Need to handle some corner case because it may create some performance issue in regular.
  // this part will impact timeseries flow. I will check with team
  def getWorkFlowTime(dt: Date, lastSuccessfulBatchComplete: Long): Time = {
    val todayDate = new Date(System.currentTimeMillis())
    if (dt.before(todayDate)) (new Time(dt.getTime))
    else new Time(System.currentTimeMillis())
  }





  def getDataDir(appConfig: ApplicationConfig, batchData: Config): String = {

    val dataDirectoryConfig = batchData.getConfig("dataDirectory")
    val directoryConfigPathEnv = getDirectoryConfigPathEnv(appConfig)

    if (appConfig.uid.equals("")) {
      if (dataDirectoryConfig.hasPath("query")) {
        appConfig.fileSystemPrefix + dataDirectoryConfig
          .getString(directoryConfigPathEnv) + dataDirectoryConfig.getString("query")
      } else {
        appConfig.fileSystemPrefix + dataDirectoryConfig
          .getString(directoryConfigPathEnv)
      }
    } else {
      if (dataDirectoryConfig.hasPath("query")) {
        appConfig.fileSystemPrefix + dataDirectoryConfig
          .getString(directoryConfigPathEnv) + dataDirectoryConfig.getString("query")
      } else {
        appConfig.fileSystemPrefix + dataDirectoryConfig
          .getString(directoryConfigPathEnv)
      }
    }
  }

  def getDirectoryConfigPathEnv(appConfig: ApplicationConfig): String = {
    if (appConfig.integrationRun) {
      "integration"
    } else {
      ApplicationUtils.env
    }
  }

  def getCurrentWorkflowTime(appConfig: ApplicationConfig,
                             workflowConfig: WorkflowConfig,
                             batchData: Config): Long = {
    if (batchData.hasPath("readFullDataset")) {
      val readFullDataset = batchData.getString("readFullDataset").toBoolean
      if (!readFullDataset) {
        ApplicationUtils.getCurrentWorkflowTime(appConfig, workflowConfig)
      } else {
        -1L
      }
    } else {
      ApplicationUtils.getCurrentWorkflowTime(appConfig, workflowConfig)
    }
  }
}

// 1. Add DAO support to read Hive and Cassandra for ModelTransaction
// 2. Add Kafka support to read the online stream data that's added to FS.
// Is the time series maintained in Kafka batch path ?
