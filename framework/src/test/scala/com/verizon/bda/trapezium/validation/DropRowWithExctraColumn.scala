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
package com.verizon.bda.trapezium.validation

import java.io.File
import java.nio.file.{Paths, Path}

import com.verizon.bda.trapezium.framework.{ApplicationManager, ApplicationManagerTestSuite}
import com.verizon.bda.trapezium.framework.manager.WorkflowConfig

/**
 * Created by parmana on 11/14/16.
 */
class DropRowWithExctraColumn  extends ApplicationManagerTestSuite {
  val currentRelativePath: Path = Paths.get("")
  val path: String = currentRelativePath.toAbsolutePath.toString
  import org.apache.commons.io.FileUtils
  FileUtils.deleteDirectory(new File(path + "/tmp/dropRowWithExtraColumn"))

  test("Extra Column Drop"){
    val workFlowToRun: WorkflowConfig =
      ApplicationManager.setWorkflowConfig("dropRowWithExtraColumn")
    ApplicationManager.runBatchWorkFlow(
      workFlowToRun,
      appConfig, maxIters = 1 )(sparkSession)
     val cnt = sc.textFile(path + "/tmp/dropRowWithExtraColumn").count()
     assert(cnt == 1, "Excpected numner of row is not correct")

  }
}
