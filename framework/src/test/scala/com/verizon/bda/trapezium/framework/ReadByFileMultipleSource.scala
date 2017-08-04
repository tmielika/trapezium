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

import java.util.{Calendar, Date}

import com.typesafe.config.{Config, ConfigObject}
import com.verizon.bda.trapezium.framework.handler.{FileCopy, FileSourceGenerator}
import com.verizon.bda.trapezium.framework.manager.WorkflowConfig
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils

/**
  * @author hutashan test read by offset
  */
class ReadByFileMultipleSource extends ApplicationManagerTestSuite {

  var startTime = System.currentTimeMillis()-500000
  override def beforeAll(): Unit = {
    super.beforeAll()
    FileCopy.fileDelete
    FileCopy.copyFiles(4)
  }

  test("readByFileFlow workflow should successfully run the batch workflow") {
    ApplicationManager.updateWorkflowTime(startTime, "readByOffset2Source")
    val workFlowToRun: WorkflowConfig = ApplicationManager.setWorkflowConfig("readByOffset2Source")
    val updatedDate = System.currentTimeMillis()-172800 *2 *1000
    ApplicationManager.updateWorkflowTime(updatedDate, "readByOffset2Source")
    ApplicationManager.runBatchWorkFlow(
      workFlowToRun,
      appConfig , maxIters = 1)(sc)

  }



}
