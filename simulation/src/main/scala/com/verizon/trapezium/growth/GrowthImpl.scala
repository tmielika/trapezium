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
package com.verizon.trapezium.growth

import com.verizon.bda.trapezium.framework.manager.WorkflowConfig
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import collection.JavaConverters._
import scala.reflect.runtime._


/**
 * Created by parmana on 10/6/16.
 */
class GrowthImpl(df: DataFrame, workflowConfig: WorkflowConfig) extends DefaultUdfs {
  val logger = LoggerFactory.getLogger(this.getClass)

  def growth(): SaveGrowth = {
    try {
      val conf = workflowConfig.workflowConfig.getConfig("dataSliceInfo")
      df.printSchema()
      df.show(false)
      val growthcol = conf.getStringList("growthcol").asScala.toArray
      val growthcolType = conf.getStringList("growthColTypes").asScala.toArray
      val repetition = conf.getInt("repetition")
      //      var df1: DataFrame = df
      val rangeUdf = udf((repetition: Int) => List.tabulate(repetition)(i => i))
      var datagrowth: DataFrame = df.withColumn("rand", rangeUdf(lit(repetition)))
        .withColumn("rand", explode(col("rand")))
      for ((inputCol, dataType) <- (growthcol zip growthcolType)) {

        def listGeneratorUdf = dataType.toLowerCase match {
          case "string" =>
            udf(
              closureFuncForString)
          case "int" =>
            udf(closureFuncForInt)
          case "double" =>
            udf(closureFuncForDouble)
          case "long" =>
            udf(closureFuncForDouble)
          case _ => {
            val clazz = customObject(dataType)
            clazz.asInstanceOf[Growth[clazz.type]].customUDF
          }
        }
        datagrowth = datagrowth.withColumn(inputCol,
          listGeneratorUdf(datagrowth(inputCol), datagrowth("rand")))
      }
      datagrowth = datagrowth.drop("rand")
      return new SaveGrowth(datagrowth  , workflowConfig)
     } catch {
      case ex: Exception => {

        logger.error("Error occured while growing the data", ex.printStackTrace())
      }
    }
    null
  }
  private def customObject(dataType: String) = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(dataType)
    val obj = runtimeMirror.reflectModule(module)
    obj.instance.asInstanceOf[CustomType]
  }
}
