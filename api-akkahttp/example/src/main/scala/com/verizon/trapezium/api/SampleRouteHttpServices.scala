package com.verizon.trapezium.api

import java.util

import com.verizon.trapezium.api.ApiSvcProcessor
import com.verizon.trapezium.api.akkahttp.ApiHttpServices
import com.verizon.trapezium.api.utils.SampleHttpSvcsConstans._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

/**
  * Created by chundch on 4/25/17.
  */

class SampleRouteHttpServices(implicit val executionContext: ExecutionContext)
                                          extends ApiHttpServices {

  private val logger = LoggerFactory.getLogger(this.getClass)



  override def getProcessors: util.HashMap[String, ApiSvcProcessor] = {

    val processors = new util.HashMap[String, ApiSvcProcessor]

    processors.put(SAMPLE_HTTP_SERVICE_RESOURCE, new SampleRouteProcessor)
    processors.put(SAMPLE_HTTP_SERVICE_RESOURCE + "_1", new SampleRouteProcessor)

    logger.info("Created processors list, number of processors : " + processors.size())
    processors

  }

  override def getApiSvcAuthorizer: String = {
    EXAMPLE_SVCS_ROUTE_AUTHORIZER
  }

}
