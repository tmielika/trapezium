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
package com.verizon.bda.trapezium.transformation

import com.typesafe.config.{ConfigObject, Config}
import com.verizon.bda.trapezium.framework.manager.WorkflowConfig
import com.verizon.bda.trapezium.framework.{ApplicationManager, ApplicationManagerTestSuite}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.{BeforeAndAfter, Matchers}

/**
  * Created by venkatesh on 10/18/16.
  */
class DataTransformerSpec extends ApplicationManagerTestSuite with Matchers with  BeforeAndAfter {
  //  var sqlContext: SQLContext = null
  var df: DataFrame = _
  before{
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._
    df = sc.parallelize(Seq("one", "two", "three", "four")).toDF("numbers")
  }
  test("testing tranform feature ") {
    val workFlowConfig = new WorkflowConfig("transformWorkFlow")
    val batchConfig = workFlowConfig.workflowConfig
      .getConfig("hdfsFileBatch").getList("batchInfo")
      .get(0).asInstanceOf[ConfigObject].toConfig
    val df1 = DataTranformer.transformDf(df, batchConfig)
    val arr = df1.select("numbers").rdd.map(r => r(0)).collect()

    arr.size should be(4)
    arr should be(Array("1", "2", "3", "sample"))
  }

  test("should make a map from a" +
    "properties file ") {
    val map = DataTranformer.readPropertiesFromLocal("src/main/resources/sample.properties")
    map("one") should be("1")
    map.size should be(6)
  }

  test("testing populateMapVal method ") {
    val map = Map("one" -> "1", "two" -> "2", "three" -> "3")
    val columnName = "numbers"
    val defaultValue = "integer"
    val df1 = DataTranformer.populateMapVal(df, map, columnName, defaultValue)
    val arr = df1.select("numbers").rdd.map(r => r(0)).collect()
    arr.size should be(4)
    arr should be(Array("1", "2", "3", "integer"))
  }
  test("transformation flow character"){
    val workFlowToRun: WorkflowConfig =
      ApplicationManager.setWorkflowConfig("testWorkFlow3")
    ApplicationManager.runBatchWorkFlow(
      workFlowToRun,
      appConfig, maxIters = 1 )(sparkSession)
  }
}
