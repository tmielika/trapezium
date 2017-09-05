package com.verizon.bda.trapezium.framework.server

import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import org.apache.spark.zookeeper.EmbeddedZookeeper
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Created by Pankaj on 8/24/17.
 */
class HttpServerSuiteBase extends FunSuite with BeforeAndAfterAll {

  var appConfig: ApplicationConfig = _
  var zk: EmbeddedZookeeper = null

  override def beforeAll(): Unit = {
    appConfig = ApplicationManager.getConfig()

    // set up ZooKeeper Server
    zk = new EmbeddedZookeeper(appConfig.zookeeperList.split(",")(0))

  }

  override def afterAll: Unit = {
    // Close ZooKeeper connections
    ZooKeeperConnection.close

    if (zk != null) {
      zk.shutdown()
      zk = null
    }

  }

}
