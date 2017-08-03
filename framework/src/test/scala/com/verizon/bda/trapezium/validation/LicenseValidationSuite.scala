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
