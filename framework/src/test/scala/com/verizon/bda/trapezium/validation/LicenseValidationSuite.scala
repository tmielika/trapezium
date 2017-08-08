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
import com.verizon.bda.license.ZookeeperClient
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection

class LicenseValidationSuite extends TestSuiteBase {

  val logger = LoggerFactory.getLogger(this.getClass)

  var zk: ZooKeeper = null
  var appConfig: ApplicationConfig = _
  var zookeeperClient : ZookeeperClient = _

  override def beforeAll() {
    super.beforeAll()
    appConfig = ApplicationManager.getConfig()
    zookeeperClient = new ZookeeperClient("localhost:2181")
  }

  def hexStringToByteArray(s: String): Array[Byte] = {
    var len = s.length
    var data = new Array[Byte](len / 2)
    var i = 0
    while ( i < len) {
      data(i/2) = ((Character.digit(s.charAt(i), 16) << 4)
        + Character.digit(s.charAt(i + 1), 16)).toByte
      i += 2
    }
    data
  }

  test("Test License Validation") {

    var sampleLicense: String = "0000001000000020000000b17be89cd1f3bfe938fdc72753d080" +
      "77e649545b47f7449c8fa49031eb0376f606253719012d4ff14557d1641a535666b11aa7f3431853d4d" +
      "8125640fafa11b27092ceab086c15ee90c6a33627232ebe89d1796d03467b47bc55c3490a528cfbfe89" +
      "c86ff6c961d6c953f3ea3eca61a12ff901f481fd96e972ebe72c837fcacb59b733d06cf6c16ad2dde2e" +
      "7d6871821a59b6300c3ee4c08266353c2f009978361eabb374f104d4b02b6b314224328be4fb870950a" +
      "c698177018b0df68dd867ebc5e0554fd53ea47d34b3d5054d22ef06b8e063692d4d5ab26e92f48bd39f0fa23f2"

    val b = hexStringToByteArray(sampleLicense)

    val port = zookeeperClient.getDynamicPort

    val zk1 = connectToZk("localhost:" + port)

    try
      zk1.delete("/bda/licenses/platform/orion_customer1", 0)
    catch {
      case e: KeeperException =>
        logger.error("Directory is not presented ", e)
    }

    try
      zk1.create("/bda", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    catch {
      case e: KeeperException =>
        logger.error("Directory already presented /bda ", e)
    }
    try
      zk1.create("/bda/licenses", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    catch {
      case e: KeeperException =>
        logger.error("Directory already presented /bda/licenses ", e)
    }
    try
      zk1.create("/bda/licenses/platform", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    catch {
      case e: KeeperException =>
        logger.error("Directory already presented /bda/licenses/platform ", e)
    }
    try
      zk1.create("/bda/licenses/platform/orion_customer1", b,
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    catch {
      case e: KeeperException =>
        logger.error("Directory is not presented ", e)
    }

    LicenseLib.init(appConfig.zookeeperList)
    logger.info("init done")
    val validLicense = LicenseLib.isValid(LicenseType.PLATFORM)
    logger.info("validLicense =" + validLicense)
    assert(validLicense == true)
  }

  def connectToZk(zkServers: String) : ZooKeeper = {
    val ZkConnectTimeout: Int = 2400000
    try {
      if (zk == null) {
        zk = new ZooKeeper(zkServers, ZkConnectTimeout, new ZookeeperWatcher)
      }
      if (zk == null) {
        logger.error("Failed to connect to Zookeeper at " + zkServers, "")
      }
    } catch {
      case var3: IOException =>
        logger.error("Failed to connect to Zookeeper", var3.getStackTrace)
        throw new LicenseException("Failed to connect to Zookeeper")
    }
    while ( {
      !(zk.getState == States.CONNECTED)
    }) {
      logger.info("Connecting to Zookeeper...")
      try
        Thread.sleep(1000L)
      catch {
        case var2: InterruptedException =>
          logger.error("Interrupted while waiting to connect to Zookeeper", var2.getStackTrace)
          throw new LicenseException("Unable to connect to Zookeeper")
      }
    }
    logger.info("Connected to Zookeeper")
    zk
  }

  override def afterAll() {
    // Close ZooKeeper connections
    LicenseLib.close()
    if (zookeeperClient != null) zookeeperClient.shutdown()
  }

}
