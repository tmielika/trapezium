package com.verizon.trapezium.api.akkahttp.server.test

import akka.http.scaladsl.model.StatusCodes
import com.verizon.trapezium.api.akkahttp.server.HttpServicesResponse
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.slf4j.LoggerFactory

/**
  * Created by chundch on 5/1/17.
  */


@RunWith(classOf[JUnitRunner])
class TestHttpServicesResponse  extends FunSuite with BeforeAndAfterAll {

  private val logger = LoggerFactory.getLogger(this.getClass)


  /**
    * Helper to initialize required
    * object for the test
    */

  override def beforeAll() {
    super.beforeAll()

  }

   /**
    * Test getAkkaHttpResponse method
    */

   test("http resonse for a given message") {
     logger.info("Testing getAkkaHttpResponse of HttpServicesResponse")
     val httpSvcResp = new HttpServicesResponse
     val message = "test http response"
     val httpRespCode = StatusCodes.OK
     val akkaHttpResp = httpSvcResp.getAkkaHttpResponse(httpRespCode , message)
     assert(akkaHttpResp.status.equals(httpRespCode))
  }



}
