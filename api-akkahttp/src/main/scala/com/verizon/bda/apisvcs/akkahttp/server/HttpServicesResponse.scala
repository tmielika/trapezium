package com.verizon.bda.apisvcs.akkahttp.server

import akka.http.scaladsl.model.{HttpResponse, StatusCode}
import com.verizon.logger.BDALoggerFactory

/**
  * Created by chundch on 4/25/17.
  */
class HttpServicesResponse {

  private val logger = BDALoggerFactory.getLogger(this.getClass)

  def getAkkaHttpResponse(statusCode: StatusCode, responseMessage : String) : HttpResponse = {
     logger.debug("creating http response with status code : " + statusCode.toString())
      HttpResponse(statusCode, entity = responseMessage)
  }

}
