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

import java.nio.file.{Paths, Path}
import java.util.Timer
import org.apache.spark.zookeeper.ChaosMonekyUtils

/**
 * Created by parmana on 1/10/17.
 */
class ChaosMonkeyTestSuite extends ApplicationManagerTestSuite{

  val currentRelativePath: Path = Paths.get("")
  val path: String = currentRelativePath.toAbsolutePath.toString
  test("Test Chaos Monkey") {
    // ApplicationManager.main(Array("--config", "conf", "--workflow", "dataslice_workflow"));
    logger.info("Test Chaos Monkey")
    val chparsam = appConfig.chaosMonkeyParam
    val delay = {
      if (chparsam.hasPath("delay")) {
        chparsam.getInt("delay")
      } else {
        10
      }
    }
    val frequncyToKillExecutor = chparsam.getInt("frequncyToKillExecutor")
    val timer = new Timer()
    val ch = new ChaosMonekyUtils(sc.applicationId
      , appConfig.chaosMonkeyParam , timer)(sc)
    timer.scheduleAtFixedRate(ch , delay*1000 , frequncyToKillExecutor*1000);
    Thread.sleep(10000)
  }



}
