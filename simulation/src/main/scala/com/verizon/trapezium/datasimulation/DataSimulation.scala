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
package com.verizon.trapezium.datasimulation

import java.sql.{Date, Time}

import com.verizon.bda.trapezium.framework.{Trigger, ApplicationManager, BatchTransaction}
import org.apache.spark.sql.DataFrame
import org.joda.time.format.{ DateTimeFormat, DateTimeFormatter }
import org.slf4j.LoggerFactory

/**
 * Created by parmana on 4/1/16.
 */
object DataSimulation extends BatchTransaction {

    /**
   * method to perform processing
   * @param df
   * @param workflowTime
   * @return
   */
    val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame],
                            workflowTime: Time): DataFrame = {
      val data: DataFrame = df("datasimulation")

    data
  }
  /**
   * method to persist the data
   * @param data
   * @param batchTime
   */
  override def persistBatch(data: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    try {
    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.getWorkflowConfig()
     Simulation.simulation(data, workflowConfig)
      None
    } catch {
      case ex: Exception => {
        logger.error("Error while datasimulation the data", ex.printStackTrace())
        None
      }
    }
  }
}
