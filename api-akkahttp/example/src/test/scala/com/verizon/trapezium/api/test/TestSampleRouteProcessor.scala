package com.verizon.trapezium.api.test

import akka.util.ByteString
import com.verizon.trapezium.api.SampleRouteProcessor
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by chundch on 5/1/17.
  */

@RunWith(classOf[JUnitRunner])
class TestSampleRouteProcessor extends FunSuite with BeforeAndAfterAll {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Helper to initialize required
    * object for the test
    */

  override def beforeAll() {
    super.beforeAll()

  }


  /**
    * Test process method
    */

  test("validate process ") {

    logger.info("Testing process method")
    val sampleRouteServices = new SampleRouteProcessor
    val path = "test"
    val pathVersion = "v1"
    val reqData = null
    val reqContent = "{ \"sampling_rate\": 1,\"n\": 100," +
      " \"events\": [{ \"profileId\": \"profileid\", \"ip\": \"uymi\"}]}"
    val reqDatInput = ByteString(reqContent).iterator.asInputStream
    val response = sampleRouteServices.process(path, pathVersion, reqData, reqDatInput )
    logger.info("response from process : " + response )
    assert(response.equals(reqContent))

  }


}
