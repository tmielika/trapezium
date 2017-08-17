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
package com.verizon.trapezium.dataslice

import java.sql.Time

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.{Trigger, ApplicationManager, BatchTransaction}
import com.verizon.trapezium.growth.{Slice, Growth}
import com.verizon.trapezium.utils.Util
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
 * Created by parmana on 4/1/16.
 */
object DataSlicer  extends BatchTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)
  /**
   * method to perform processing
   * @param df
   * @param workflowTime
   * @return
   */
  var counter : Int = 0
  override def processBatch(df: Map[String, DataFrame],
                            workflowTime: Time): DataFrame = {
    val dfm = df("datasliceworkflow")
    dfm
  }
  /**
   * method to persist the data
   * @param df
   * @param batchTime
   */
  override def persistBatch(df : DataFrame , batchTime : Time): Option[Seq[Trigger]] = {
 try {
   logger.info("applicationId==>" + df.rdd.sparkContext.applicationId)
   counter = counter + 1
   val appConfig = ApplicationManager.getConfig()
   val pa = appConfig.kafkabrokerList
   val workflowConfig = ApplicationManager.getWorkflowConfig()
   val conf: Config = workflowConfig.workflowConfig.getConfig("dataSliceInfo")
   val runmode = conf.getStringList("slicemode")
   val sqlContext = df.sqlContext
   sqlContext.udf.register("quantizeUdf" , Util.quantizeUdf(_: String , _: Long))
   if (runmode.contains("slice") && runmode.contains("growth")){
     logger.info("mode is slice and growth")
     new Growth(df, workflowConfig).growth().slice().saveToHdfs()
   } else if (runmode.contains("slice")){
     logger.info("mode is slice")
     new Slice(df, workflowConfig).slice().saveToHdfs()
   } else if (runmode.contains("growth")){
     logger.info("mode is slice")
     new Growth(df, workflowConfig).growth().saveToHdfs()
   } else {
     logger.error("Please specify the correct mode valid modes are : " +
       " slice,growth", "Please " +
       " specify" +
       " the correct mode valid modes are : slice,growth")
   }
   logger.info("Transaction Done")
   None
    } catch {
   case ex : Exception => {
     logger.error("Error while slicing the data" , ex.printStackTrace())
     None
   }
 }
  }



}
