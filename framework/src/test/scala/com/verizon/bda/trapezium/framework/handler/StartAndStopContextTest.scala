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

import com.verizon.bda.trapezium.framework.{ApplicationManager, ApplicationManagerTestSuite}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * @author hutashan start and stop context
  */
class StartAndStopContextTest extends ApplicationManagerTestSuite {


  test("Start Context test") {
    val logger = LoggerFactory.getLogger(this.getClass)
    val thread1 = new WFThread(sparkSession, "batchWorkFlow")
    thread1.start()
    while (thread1.isRunning) {
      Thread.sleep(100)
    }
    logger.info("check context. Context value" + sc.isStopped)
    assert(!sc.isStopped)
  }

  test("Start Context") {
    val logger = LoggerFactory.getLogger(this.getClass)
    if (sc!=null && !sc.isStopped) {
      sc.stop()
      sc = null
      logger.info("Context stopped")
    }
    val workflowConfig = ApplicationManager.setWorkflowConfig("batchWorkFlow")
    val batchHandler = new BatchHandler(workflowConfig, appConfig, 1)(sparkSession)
    val sc1 = batchHandler.createSession.sparkContext
    assert(!sc1.isStopped)
    if (!sc1.isStopped) {
      sc1.stop()
      logger.info("Context stopped")
    }
  }


  override def afterAll(): Unit = {
    super.afterAll()
  }

}
  class WFThread(sparkSession: SparkSession, wf: String) extends Thread {
    val logger = LoggerFactory.getLogger(this.getClass)
    var isRunning = true

    override def run(): Unit = {

      val workflowConfig = ApplicationManager.setWorkflowConfig(wf)
      logger.info(" ####################Job Start ######  " + wf + " ####################")
      ApplicationManager.runBatchWorkFlow(
        workflowConfig, ApplicationManager.getConfig(), 1)(sparkSession)
      logger.info(" ####################Job completed ######  " + wf + " ####################")
      isRunning = false
    }


}

