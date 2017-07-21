package com.verizon.bda.trapezium.validation

import java.io.IOException

import com.verizon.bda.license._
import org.apache.spark.streaming.TestSuiteBase
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.{CreateMode, KeeperException, ZooKeeper}
import org.slf4j.LoggerFactory

class LicenseCustomValidationSuite extends TestSuiteBase {

  val logger = LoggerFactory.getLogger(this.getClass)
  var zk: ZooKeeper = null

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

  test("Test Custome License Validation") {
    /* Generating license manually and placing in Zookeeper, LicenseLib APIs should return
           true for license validity checks &
           features f1, f2, f3, f4, validate With a license for API and features f1, f2, f3, f4, validate
           that the licenseLib APIs work.
         */

    var sampleLicense: String = "0000001000000020000000b17be89cd1f3bfe938fdc72753d080" +
      "77e649545b47f7449c8fa49031eb0376f606253719012d4ff14557d1641a535666b11aa7f3431853d4d" +
      "8125640fafa11b27092ceab086c15ee90c6a33627232ebe89d1796d03467b47bc55c3490a528cfbfe89" +
      "c86ff6c961d6c953f3ea3eca61a12ff901f481fd96e972ebe72c837fcacb59b733d06cf6c16ad2dde2e" +
      "7d6871821a59b6300c3ee4c08266353c2f009978361eabb374f104d4b02b6b314224328be4fb870950a" +
      "c698177018b0df68dd867ebc5e0554fd53ea47d34b3d5054d22ef06b8e063692d4d5ab26e92f48bd39f0fa23f2"

    var b = hexStringToByteArray(sampleLicense)
    val zk = connectToZk("127.0.0.1:2181")

    try {
      zk.delete("/bda/licenses/api", 0)
      zk.delete("/bda/licenses/platform", 0)
    }
    catch {
      case ex: KeeperException =>
        logger.error("Directory is not presented ", ex)
    }

    zk.create("/bda/licenses/platform", b, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    zk.create("/bda/licenses/api", b, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

    LicenseLib.init("127.0.0.1:2181")

    val platform = LicenseLib.isValid(LicenseType.PLATFORM)
    val api = LicenseLib.isValid(LicenseType.API)

    logger.info( "platform =" + platform)
    logger.info( "api =" + api)

    assert(platform == true)
    assert(api == true)

    // Check license for a feature
    assert(true == LicenseLib.featureEnabled(LicenseType.API, "f1"))
    assert(true == LicenseLib.featureEnabled(LicenseType.API, "f2"))
    assert(true == LicenseLib.featureEnabled(LicenseType.API, "f3"))
    assert(true == LicenseLib.featureEnabled(LicenseType.API, "f4"))
    assert(false == LicenseLib.featureEnabled(LicenseType.API, "f5"))


  }

  test("License Validation"){
    /* With no licenses in Zookeeper, LicenseLib APIs should return
           false for license validity checks
         */

    val zk = connectToZk("127.0.0.1:2181")

    try {
      zk.delete("/bda/licenses/api", 0)
      zk.delete("/bda/licenses/platform", 0)
    }
    catch {
      case ex: KeeperException =>
        logger.error("Directory is not presented ", ex)
    }

    LicenseLib.init("127.0.0.1:2181")

    val platform = LicenseLib.isValid(LicenseType.PLATFORM)
    val api = LicenseLib.isValid(LicenseType.API)

    logger.info( "platform =" + platform)
    logger.info( "api =" + api)

    assert(platform == false)
    assert(api == false)

  }

  def connectToZk(zkServers: String) : ZooKeeper = {
    val ZkConnectTimeout: Int = 6000
    try {
        if (zk == null) {
          zk = new ZooKeeper("127.0.0.1:2181", ZkConnectTimeout, new ZookeeperWatcher)
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
        print("Connecting to Zookeeper...")
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

  }
