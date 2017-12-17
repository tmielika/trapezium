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
package com.verizon.bda.trapezium.framework

import java.util.{Date, Calendar}
import com.typesafe.config.{ConfigObject, Config}
import com.verizon.bda.trapezium.framework.handler.{FileSourceGenerator, FileCopy}
import com.verizon.bda.trapezium.framework.manager.WorkflowConfig
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
// scalastyle:off
import scala.collection.JavaConversions._
// scalastyle:on

/**
  * @author hutashan test read by offset
  */
class ApplicationManagerReadByFileSuite extends ApplicationManagerTestSuite {
  var startTime = System.currentTimeMillis()-500000

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileCopy.fileDelete
    FileCopy.copyFiles(2)
  }

  test("readByFileFlow workflow should successfully run the batch workflow") {
    ApplicationManager.updateWorkflowTime(startTime, "readByOffset")
    val workFlowToRun: WorkflowConfig = ApplicationManager.setWorkflowConfig("readByOffset")
    val updatedDate = System.currentTimeMillis()-172800 *1000
    ApplicationManager.updateWorkflowTime(updatedDate, "readByOffset")
    ApplicationManager.runBatchWorkFlow(
      workFlowToRun,
      appConfig , maxIters = 1)(spark)
  }
  test("getElligbleFiles Test") {
    ApplicationManager.updateWorkflowTime(startTime, "readByOffset")
    val workFlowToRun: WorkflowConfig = ApplicationManager.setWorkflowConfig("readByOffset")
    val updatedDate = System.currentTimeMillis() - 86400 * 2 * 1000
    ApplicationManager.updateWorkflowTime(updatedDate, "readByOffset")
    val dt1 = new Date(System.currentTimeMillis())
    val sb = new StringBuilder("test")
    val dt2 = new Date(System.currentTimeMillis() - 86400 * 1000)
    val dt3 = new Date(System.currentTimeMillis() - 86400 * 2 * 1000)
    val dt4 = new Date(System.currentTimeMillis() - 86400 * 3 * 1000)
    val dt5 = new Date(System.currentTimeMillis() - 86400 * 4 * 1000)
    val dt6 = new Date(System.currentTimeMillis() - 86400 * 5 * 1000)
    val mapFile = new java.util.TreeMap[Date, java.util.HashMap[String, StringBuilder]]()
    val mp = new  java.util.HashMap[String, StringBuilder]()
    mp.put("test", sb)
   // val mp = Map("test" -> sb)
    mapFile.put(dt1, mp)
    mapFile.put(dt2, mp)
    mapFile.put(dt3, mp)
    mapFile.put(dt4, mp)
    mapFile.put(dt5, mp)
    mapFile.put(dt6, mp)
    val hdfsBatchConfig = workFlowToRun.hdfsFileBatch.asInstanceOf[Config]
    val batchInfoList = hdfsBatchConfig.getList("batchInfo")
    val groupFileConf = batchInfoList(0).asInstanceOf[ConfigObject].toConfig.getConfig("groupFile")

    val files = FileSourceGenerator.getElligbleFiles(mapFile,
      workFlowToRun, appConfig, groupFileConf)
    // assert(files.size() == 2)
    assert(files.firstKey() == dt3)
    val updatedDate1 = System.currentTimeMillis() - 86400 * 1 * 1000
    ApplicationManager.updateWorkflowTime(updatedDate1, "readByOffset")
    val files1 = FileSourceGenerator.getElligbleFiles(mapFile,
      workFlowToRun, appConfig, groupFileConf)
    assert(files1.size() == 0)
    val updatedDate12 = System.currentTimeMillis() - 86400 * 2 * 1000
    ApplicationManager.updateWorkflowTime(updatedDate12, "readByOffset")
    val files12 = FileSourceGenerator.getElligbleFiles(mapFile,
      workFlowToRun, appConfig, groupFileConf)
    assert(files12.size() == 1)
  }



  test("readByFileFlow testing ") {
    val updatedDate12 = System.currentTimeMillis() - 86400 * 3 * 1000
    ApplicationManager.updateWorkflowTime(updatedDate12, "readByOffset")

    var cal = Calendar.getInstance()
    cal.add(Calendar.DATE, - 1)
    val processedDate = getStartOfDay(cal.getTime).getTime
    val keyFileProcessed = s"/etl/fileSplitWorkFlow/fileProccessed"
    ApplicationUtils.updateZookeeperValue(keyFileProcessed,
      processedDate, appConfig.zookeeperList)
     val workFlowToRun: WorkflowConfig = ApplicationManager.setWorkflowConfig("readByOffset")
     ApplicationManager.runBatchWorkFlow(
      workFlowToRun,
      appConfig, maxIters = 1 )(spark)
  }

  def getStartOfDay(dt : java.util.Date) : java.util.Date = {
    val calendar = Calendar.getInstance()
    calendar.setTime(dt)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    return calendar.getTime()
  }

  override def afterAll(): Unit = {
    // Delete the temp directory
    FileCopy.fileDelete
    super.afterAll()
  }
}
