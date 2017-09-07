package com.verizon.trapezium.api.akkahttp.services.test

import java.util

import com.verizon.trapezium.api.ApiSvcProcessor
import com.verizon.trapezium.api.akkahttp.ApiHttpServices
import com.verizon.trapezium.api.akkahttp.services.processors.test.TestEndpointProcessor
import com.verizon.trapezium.api.akkahttp.utils.test.ApiServicesTestConstatns._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext


/**
  * Created by chundch on 5/22/17.
  */


class TestEndpointService(implicit val executionContext: ExecutionContext)  extends ApiHttpServices{

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def getApiSvcAuthorizer: String = {
    APISVCS_TESTENDPOINT_AUTHORIZER
  }


  override def getProcessors: util.HashMap[String, ApiSvcProcessor] = {

    val processors = new util.HashMap[String, ApiSvcProcessor]

    processors.put(APISVCS_TESTENDPOINT1_RESOURCE, new TestEndpointProcessor)
    processors.put(APISVCS_TESTENDPOINT1_RESOURCE1, new TestEndpointProcessor)

    logger.info("Created processors list, number of processors : " + processors.size())
    processors


  }
}
