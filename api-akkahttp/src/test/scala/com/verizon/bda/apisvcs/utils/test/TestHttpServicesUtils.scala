package com.verizon.bda.apisvcs.utils.test

import akka.http.scaladsl.model.StatusCodes
import com.verizon.bda.apisvcs.security.ApiAuthorizationService
import com.verizon.bda.apisvcs.utils.HttpServicesUtils
import org.slf4j.LoggerFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.ExecutionContext

/**
  * Created by chundch on 5/1/17.
  */

@RunWith(classOf[JUnitRunner])
class TestHttpServicesUtils extends FunSuite with BeforeAndAfterAll {

  private val logger = LoggerFactory.getLogger(this.getClass)


  /**
    * Helper to initialize required
    * object for the test
    */

  override def beforeAll() {
    super.beforeAll()

  }


  /**
    * Test getHostName method
    */

  test("validate getHostName ") {
    logger.info("Testing getHostName")
    val hostname = HttpServicesUtils.getHostName
    assert(hostname != null)
  }



  /**
    * Test getSuccessHttpResponse method
    */

  test("validate getSuccessHttpResponse ") {
    logger.info("Testing getSuccessHttpResponse")
    val msg = "request processed"
    val akkaresp = HttpServicesUtils.getSuccessHttpResponse(msg)
    assert(akkaresp.status.equals(StatusCodes.OK))
  }

  /**
    * Test getSuccessHttpResponse method
    */

  test("validate getFailedHttpResponse ") {
    logger.info("Testing getFailedHttpResponse")
    val failedcode = StatusCodes.BadRequest
    val msg = "processing failed"
    val akkaresp = HttpServicesUtils.getFailedHttpResponse(failedcode, msg)
    assert(akkaresp.status.equals(StatusCodes.BadRequest))
  }



}
