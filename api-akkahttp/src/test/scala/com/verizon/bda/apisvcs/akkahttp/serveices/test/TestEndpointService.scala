package com.verizon.bda.apisvcs.akkahttp.serveices.test

import java.util

import com.verizon.bda.apiservices.ApiSvcProcessor
import com.verizon.bda.apisvcs.ApiHttpServices
import com.verizon.bda.apisvcs.akkahttp.serveices.processors.test.TestEndpointProcessor
import com.verizon.bda.apisvcs.utils.test.ApiServicesTestConstatns._
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
