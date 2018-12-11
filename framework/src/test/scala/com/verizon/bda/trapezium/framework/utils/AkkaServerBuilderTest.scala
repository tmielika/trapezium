package com.verizon.bda.trapezium.framework.utils

import com.typesafe.config.{Config, ConfigFactory}
import com.verizon.bda.trapezium.framework.server.AkkaTlsServer
import org.apache.log4j.Logger
import org.apache.spark.streaming.TestSuiteBase

class AkkaServerBuilderTest extends TestSuiteBase {

  var httpServerConfig: Config = _

  lazy val log = Logger.getLogger(this.getClass)
  override def beforeAll() {
    super.beforeAll()
    val config = ConfigFactory.load("local-https-server.conf")
    httpServerConfig = config.getConfig("httpServer")
  }

  test("testBuild") {
    val embeddedServer = AkkaServerBuilder.build(httpServerConfig)
    assert(embeddedServer.isInstanceOf[AkkaTlsServer])
  }

}
