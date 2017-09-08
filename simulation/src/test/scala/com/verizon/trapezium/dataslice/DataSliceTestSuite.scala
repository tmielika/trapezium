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

import java.io.File
import java.nio.file.{Path, Paths}

import com.verizon.bda.trapezium.framework.{ApplicationManagerTestSuite, ApplicationManager}
import org.apache.spark.sql.SQLContext
import org.apache.spark.zookeeper.EmbeddedZookeeper
import org.apache.spark.{SparkConf, SparkContext}



/**
 * Created by parmana on 4/1/16.
 */
class DataSliceTestSuite extends ApplicationManagerTestSuite {
  val currentRelativePath: Path = Paths.get("")
  val s: String = currentRelativePath.toAbsolutePath.toString
  logger.info("Current relative path is: " + s)

  import org.apache.commons.io.FileUtils
  FileUtils.deleteDirectory(new File("target/dataslice"))
  FileUtils.deleteDirectory(new File("target/datasimulation"))
  test("test dataslice") {
    val workFlowConfig = ApplicationManager.setWorkflowConfig("dataslice_workflow")
    ApplicationManager.runBatchWorkFlow(workFlowConfig , appConfig , 1)(sc)
  }


  test("test DataSimulation") {
    val workflowConfig = ApplicationManager.setWorkflowConfig("datasimulationFlow")
    ApplicationManager.runBatchWorkFlow(workflowConfig , appConfig, 1)(sc)
  }


  test("data validation") {
    val sourcedata = sc.textFile(s + "/src/test/data/testdata")
    val sqlContext = new SQLContext(sc)
    val growthdata = sqlContext.read.parquet("target/dataslice")
    val sourceDataCount = sourcedata.count()
    val growthdataCount = growthdata.count()
    logger.info("sourceDataCount : " + sourceDataCount)
    logger.info("growthdataCount : " + growthdataCount)
    assert(sourceDataCount*5 == growthdataCount , "growth data is not correct")
    growthdata.select("col12").distinct().show(100 , false)
    val growthColDistinctCount = growthdata.select("col12").distinct().count()
    logger.info("growthColDistinctCount : " + growthColDistinctCount)
    assert(5 == growthColDistinctCount , "Growth column has not correct data")
    val datasimulationSource = sc.textFile("target/datasimulation/*/*")
    val datasimulationcount = datasimulationSource.count()
    logger.info("datasimulationcount : " + datasimulationcount)
    assert(datasimulationcount == growthdataCount , "Growth and simulation count is not matching")
  }
}
