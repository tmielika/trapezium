package com.verizon.trapezium.api.akkahttp.server

import akka.http.scaladsl.model.{HttpResponse, StatusCode}
import org.slf4j.LoggerFactory

/**
  * Created by chundch on 4/25/17.
  */
class HttpServicesResponse {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def getAkkaHttpResponse(statusCode: StatusCode, responseMessage : String) : HttpResponse = {
     logger.debug("creating http response with status code : " + statusCode.toString())
      HttpResponse(statusCode, entity = responseMessage)
  }

}
