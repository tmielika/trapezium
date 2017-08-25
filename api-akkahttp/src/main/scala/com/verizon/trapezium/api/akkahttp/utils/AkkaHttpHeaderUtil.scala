package com.verizon.trapezium.api.akkahttp.utils

import akka.http.scaladsl.model.HttpRequest
import org.slf4j.LoggerFactory

/**
  * Created by chundch on 4/26/17.
  */

class AkkaHttpHeaderUtil (request: HttpRequest) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Akka Http header util to retrieve
    * http headers and create a Map with
    * header entries.
    * @return a Map with http header data
    */

  def headerEntries(): Map[String, String] = {
    logger.info("Creating the http headers map")
    var headersMap = Map[String, String]()

    request.headers.map( header => {

      headersMap += (header.name() -> header.value())
     }
    )
    logger.info("Number of http headers in the map : " + headersMap.size)
    headersMap
  }

  /**
    * Helper method to retrieve specific
    * http header data from akka http request
    * header.
    * @param headerKeys
    * @return a Map with values from http header for
    *         the header entry list.
    */

  def headerData(headerKeys : List[String]) : Map[String, String] = {

    logger.info("Getting http headers for : " + headerKeys.size + " number of header keys")

    var headerValuesInfo = Map[String, String]()

    headerKeys.foreach( headerkey => {
      headerValuesInfo += (headerkey -> request.getHeader(headerkey).get.value())
    })

    logger.info("Created http headers map for keys list, map size : " + headerValuesInfo.size )
      headerValuesInfo
  }


}
