package com.verizon.bda.apisvcs.akkahttp.server.test

import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import com.verizon.bda.apisvcs.akkahttp.server.HttpServicesResponse
import com.verizon.logger.BDALoggerFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers, WordSpec}

/**
  * Created by chundch on 5/1/17.
  */


@RunWith(classOf[JUnitRunner])
class TestHttpServicesResponse  extends FunSuite with BeforeAndAfterAll {

  private val logger = BDALoggerFactory.getLogger(this.getClass)


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
