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

import java.io._
import java.util.regex.Pattern
import com.typesafe.config.{ConfigFactory, Config, ConfigObject}
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.{WorkflowConfig, ApplicationConfig}
import com.verizon.bda.trapezium.framework.utils.{ScanFS, ApplicationUtils}
import com.verizon.bda.trapezium.transformation.DataTranformer
import com.verizon.bda.trapezium.validation.{DataValidator, ValidationConfig}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.collection.mutable.{Map => MMap}
import java.sql.Time
import java.text.SimpleDateFormat
import java.util.{GregorianCalendar, Calendar, Date}
import org.joda.time.Days
import org.joda.time.DateTime

// scalastyle:off
import scala.collection.JavaConversions._
// scalastyle:on

/**
 * @author sumanth.venkatasubbaiah Utility to construct RDD's from files
 *         debasish83 Added SourceGenerator which constructs iterator of time and RDD to support
 *                    reading FS by timeStamp, fileName, reading DAO for model transaction and
 *                    read kafka to get the batch data based on custom processing logic
 *        Hutashan Added file split
 */
private[framework] abstract class SourceGenerator(workflowConfig: WorkflowConfig,
                                                  appConfig: ApplicationConfig,
                                                  sc: SparkContext) {
  val logger = LoggerFactory.getLogger(this.getClass)
  val inputSources = scala.collection.mutable.Set[String]()
  var mode = ""
  // Get all the input source name
  val transactionList = workflowConfig.transactions
  transactionList.asScala.foreach { transaction: Config =>
    val txnInput = transaction.getList("inputData")
    txnInput.asScala.foreach { source =>
      val inputSource = source.asInstanceOf[ConfigObject].toConfig
      val inputName = inputSource.getString("name")
      inputSources += inputName

    }
  }
  // end txn list

  val hdfsBatchConfig = workflowConfig.hdfsFileBatch.asInstanceOf[Config]
  val batchInfoList = hdfsBatchConfig.getList("batchInfo")
  if (batchInfoList(0).asInstanceOf[ConfigObject].toConfig.hasPath("groupFile")){
    mode = "groupFile"
  }
}

object SourceGenerator {
  val logger = LoggerFactory.getLogger(this.getClass)
  def validateRDD(rdd: RDD[Row],
                  batchData: Config): DataFrame = {

    val appConfig = ApplicationManager.getConfig()
    val workFlowConfig = ApplicationManager.getWorkflowConfig
    val inputName = batchData.getString("name")

    val validationConfig =
      ValidationConfig.getValidationConfig(
        appConfig, workFlowConfig, inputName)
   val validator = DataValidator(rdd.sparkContext, inputName)
    validator.validateDf(rdd, validationConfig)
  }

  def getFileFormat(batchData: Config): String = {
    try {
      batchData.getString("fileFormat")
    } catch {
      case ex: Throwable => {
        logger.warn(s"No file format present. Using default text")
        "text"
      }
    }
  }
}
 object DynamicSchemaHelper {
   val logger = LoggerFactory.getLogger(this.getClass)
  import org.apache.spark.rdd.RDD
  def generateDataFrame(csv: RDD[Row],
                        name: String,
                        file: String,
                        sc: SparkContext): (String, DataFrame) = {
    val filename = new File(file).getName()
    var config: Config = null

    var conffilename: String = null
    var conffilecontents: String = null
    logger.info("Input file path:" + file)
    if (file.startsWith("file:")) {
      conffilename = file.stripPrefix("file:") + ".conf"
      logger.info("File path" + conffilename)
      config = ConfigFactory.parseFile(new File(conffilename))
      conffilecontents = FileUtils.readFileToString(new File(conffilename))
    }
    else
    {

      conffilename = file + ".conf"
      val pt: Path = new Path(conffilename)
      var fs: FileSystem = null
      var br: BufferedReader = null
      try {
        fs = FileSystem.get(new Configuration())
        br = new BufferedReader(new InputStreamReader(fs.open(pt)))
        config = ConfigFactory.parseReader(br)
        val writer: Writer = new StringWriter()
        IOUtils.copy(fs.open(pt), writer, "UTF-8")
        conffilecontents = writer.toString
      }
      finally
      {
        if (br != null)
           {
             br.close()
           }
       if (fs != null)
           {
             fs.close()
           }
      }

    }
    val validator = DataValidator(csv.sparkContext, name)
    logger.info(config.toString)
    logger.info("Config file path:" + conffilename)
    (conffilecontents,
      validator.validateDf(csv, config.getConfig("validation")))
  }

}

/**
 *
 * @param workflowConfig the workflow for which RDD's needs to be created
 * @param appConfig Application config
 * @param sc Spark Context instance
 */
// TO DO :
private[framework] case class
FileSourceGenerator(workflowConfig: WorkflowConfig,
                    appConfig: ApplicationConfig,
                    sc: SparkContext)  // readFullDataset is true then run only 1 times
  extends SourceGenerator(workflowConfig, appConfig, sc) {

  import FileSourceGenerator.getSortedFileMap
  import FileSourceGenerator.getWorkFlowTime

  var mapFile: java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]] = null
  var mapFileAllFile: java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]] = null

  import FileSourceGenerator.getElligbleFiles
  import FileSourceGenerator.getDataDir
  import FileSourceGenerator.getCurrentWorkflowTime

  /**
   *
   * @return Map of (String, RDD[Row]) and associated workflowTime for the data
   */
  def get(): Seq[(Time, (MMap[String, DataFrame], String))] = {

    val dataSources = new java.util.TreeMap[Time, (MMap[String, DataFrame], String)]
    val dataSourcesNoSplit = MMap[String, DataFrame]()

    var workflowTime = new Time(System.currentTimeMillis)

    // iterate over each data source
    batchInfoList.asScala.foreach { batchConfig =>

      val batchData: Config = batchConfig.asInstanceOf[ConfigObject].toConfig
      val name = batchData.getString("name")

      val dataDir = getDataDir(appConfig, batchData)

      val currentWorkflowTime = getCurrentWorkflowTime(appConfig, workflowConfig, batchData)

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
          val dataMap = addDF(sc, files.toString().split(","), batchData)

          val existingDataMap = dataSources.get(workflowTime)
          if (existingDataMap != null) {

            logger.info(s"Entry exists for this workflow time ${existingDataMap}")
            existingDataMap._1 ++= dataMap
            dataSources.put(workflowTime, (existingDataMap._1, mode))

          } else {
            dataSources.put(workflowTime, (dataMap, mode))
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
          val dataMap = addDF(sc, batchFiles, batchData)

          dataSourcesNoSplit ++= dataMap
        }
      }
    }

    val iter = dataSources.iterator
    iter.foreach( dataSource => {
      // Add no split source to other sources
      dataSource._2._1 ++= dataSourcesNoSplit
    })

    if(dataSources.isEmpty) {

        dataSources.put(workflowTime, (dataSourcesNoSplit, mode))
    }
    logger.info(s"dataSources --> ${dataSources}")
    dataSources.toMap.toSeq
  }


  import DynamicSchemaHelper.generateDataFrame
  def  addDF (sc: SparkContext,
              input: Array[String],
              batchData: Config): MMap[String, DataFrame] = synchronized{

    var dataMap = MMap[String, DataFrame]()

    val name = batchData.getString("name")
    SourceGenerator.getFileFormat(batchData).toUpperCase match {
      case "PARQUET" => {
        logger.info(s"input source is Parquet")
        dataMap += ((name, SQLContext.getOrCreate(sc).read.parquet(input: _*)))
      }
      case "AVRO" => {
        logger.info(s"input source is Avro")
        dataMap += ((name, SQLContext.getOrCreate(sc).read.format("com.databricks.spark.avro")
          .load(input: _*)))
      }
      case "DYNAMICSCHEMA" => {
        val configfiles = input.filter(filename => filename.endsWith(".conf"))
        input.filter(filename => !filename.endsWith(".conf")).map((file : String) =>
        {
          logger.info("FileName" + file)
          dataMap +=
            (generateDataFrame(sc.textFile(Array(file).mkString(",")).
              map(line => Row(line.toString)), name, file, sc))
        }
        )
        dataMap
      }
      case "JSON" => {
        logger.info(s"input source is Json")
        dataMap += ((name, SQLContext.getOrCreate(sc).read.text(input: _*)))
      }
      case _ => {

        val rdd = sc.textFile(input.mkString(",")).map(line => Row(line.toString))
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
  import DynamicSchemaHelper.generateDataFrame
  val logger = LoggerFactory.getLogger(this.getClass)

  def  getElligbleFiles(fileMap: java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]],
                        workflowConfig: WorkflowConfig,
                        appConfig: ApplicationConfig,
                        offsetConfig: Config):
         java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]] = {
    val batchTime: Long =
      workflowConfig
        .hdfsFileBatch
        .asInstanceOf[Config]
        .getLong("batchTime")
    val startOffset : Long = offsetConfig.getLong("offset")* batchTime *1000
    logger.info("startOffset :" + startOffset )
    val calculateFolderTimeStamp: Long = System.currentTimeMillis() + startOffset
    val endDate = new Date(calculateFolderTimeStamp)
    val startDate = new Date(ApplicationUtils.getCurrentWorkflowTime(appConfig, workflowConfig))
    logger.info("Date range is startDate :" + startDate + " enddate : " + endDate)
    // TO-DO may be better implementation
    val mapFile = new java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]]()
    fileMap.foreach(f => {
     val x = f._1
     val fileList = f._2
      if ((x.after(startDate) ) && (x.equals(endDate)  || x.before(endDate))){
        mapFile.put(x , fileList)
      }
    })

    mapFile
  }



/**
          * This method invoked only once in each cycle if filesplit is required.
          * Sorted files according to date
  */




  def getSortedFileMap(source : String,
                       inputFileList: java.util.List[String],
                       groupFileConf: Config,
                       workSpaceDataDirectory : String,
                       mapFile : java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]]
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
            val hs = new java.util.HashMap[String, StringBuilder] ()
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
            if (matcher.find()){
              matcher.group()
            } else ""
          }
          val dt: Date = simpleDateFormat.parse(fileDate)
          val sourceMap = mapFile.get(dt)
          if (sourceMap == null) {
            val hs = new java.util.HashMap[String, StringBuilder] ()
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
        // TO DO
        // Need to handle some corner case because it may create some performance issue in regular.
        // this part will impact timeseries flow. I will check with team


        def getWorkFlowTime(dt: Date, lastSuccessfulBatchComplete: Long): Time = {
          val todayDate = new Date(System.currentTimeMillis())
          if (dt.before(todayDate)) (new Time(dt.getTime))
          else new Time(System.currentTimeMillis())
        }



  def getDataDir (appConfig: ApplicationConfig, batchData: Config): String = {

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
  def getDirectoryConfigPathEnv(appConfig: ApplicationConfig) : String = {
    if (appConfig.integrationRun){
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
