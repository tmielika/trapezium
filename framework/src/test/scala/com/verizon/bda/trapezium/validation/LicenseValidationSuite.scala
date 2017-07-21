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

import java.io.IOException

import com.verizon.bda.license._
import com.verizon.bda.trapezium.framework.ApplicationManager.{appConfig, validLicense}
import org.apache.spark.streaming.TestSuiteBase
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.{CreateMode, ZooKeeper}
import org.slf4j.LoggerFactory
import org.apache.zookeeper.KeeperException

class LicenseValidationSuite extends TestSuiteBase {

  val logger = LoggerFactory.getLogger(this.getClass)

  test("Test License Validation") {
    LicenseLib.init("localhost:2181")
    print("init done")
    val validLicense = LicenseLib.isValid(LicenseType.PLATFORM)
    logger.info("validLicense =" + validLicense)
    assert(validLicense == true)
  }
}
