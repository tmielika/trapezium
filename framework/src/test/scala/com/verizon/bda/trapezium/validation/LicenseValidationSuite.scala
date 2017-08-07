package com.verizon.bda.trapezium.validation

import java.io.IOException

import com.verizon.bda.license._
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.ApplicationManager.{appConfig, validLicense}
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import org.apache.spark.streaming.TestSuiteBase
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.{CreateMode, ZooKeeper}
import org.slf4j.LoggerFactory
import org.apache.zookeeper.KeeperException

class LicenseValidationSuite extends TestSuiteBase {

  val logger = LoggerFactory.getLogger(this.getClass)

  var zk: ZooKeeper = null
  var appConfig: ApplicationConfig = _

  override def beforeAll() {
    super.beforeAll()
    appConfig = ApplicationManager.getConfig()
  }

  test("Test License Validation") {
    LicenseLib.init(appConfig.zookeeperList)
    logger.info("init done")
    val validLicense = LicenseLib.isValid(LicenseType.PLATFORM)
    logger.info("validLicense =" + validLicense)
    assert(validLicense == true)
  }
}
