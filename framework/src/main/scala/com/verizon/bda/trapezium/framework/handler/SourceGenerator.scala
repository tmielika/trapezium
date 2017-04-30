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

import com.typesafe.config.{Config, ConfigObject}
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.{WorkflowConfig, ApplicationConfig}
import com.verizon.bda.trapezium.validation.{DataValidator, ValidationConfig}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters.asScalaBufferConverter
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
  if (batchInfoList(0).asInstanceOf[ConfigObject].toConfig.hasPath("groupFile")) {
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
