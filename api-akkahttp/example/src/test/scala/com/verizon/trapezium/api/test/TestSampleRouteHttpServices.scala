package com.verizon.trapezium.api.test

import com.verizon.trapezium.api.SampleRouteHttpServices
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by chundch on 5/1/17.
  */

@RunWith(classOf[JUnitRunner])
class TestSampleRouteHttpServices extends FunSuite with BeforeAndAfterAll {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Helper to initialize required
    * object for the test
    */

  override def beforeAll() {
    super.beforeAll()

  }

  /**
    * Test getProcessors method
    */

  test("validate getProcessors ") {

    logger.info("Testing getProcessors")
    val sampleRouteServices = new SampleRouteHttpServices

    val noOfProcessors = 2

    assert(noOfProcessors == sampleRouteServices.getProcessors.size())

  }




}
