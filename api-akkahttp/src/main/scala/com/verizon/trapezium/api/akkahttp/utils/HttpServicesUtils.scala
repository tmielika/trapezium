package com.verizon.trapezium.api.akkahttp.utils

import java.net.InetAddress

import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCode, StatusCodes}
import com.verizon.trapezium.api.akkahttp.server.HttpServicesResponse
import com.verizon.trapezium.api.akkahttp.utils.HttpServicesConstants._
import org.slf4j.LoggerFactory

/**
  * Created by chundch on 4/25/17.
  */

object HttpServicesUtils {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Get the host name or ip address
    * that would be used in http server
    * to bind.
    * @return host name of the local host
    */

  def getHostName: String = {
    var host: String = ""
    try {
      val iAddress: InetAddress = InetAddress.getLocalHost
      host = iAddress.getHostName
      // To get  the Canonical host name
      val canonicalHostName: String = iAddress.getCanonicalHostName
       logger.info("HostName :" + host)
       logger.debug("Canonical Host Name:" + canonicalHostName)
    } catch {
      case e: Exception => {
        logger.error("Failed get host name ", e)
        throw e
      }
    }
    host
  }


  /**
    * Helper to create a Akka Http Response
    * from a string data and Akka Status Code
    * @param serviceMessage
    * @return HttpResponse
    */

  def getSuccessHttpResponse(serviceMessage: String ) : HttpResponse = {

    val response = new HttpServicesResponse
    response.getAkkaHttpResponse(StatusCodes.OK, serviceMessage)

  }

  /**
    * Helper to create a Akka Http Response
    * from a string message and a Akka Status Code.
    * @param statusCode
    * @param failedServiceMsg
    * @return HttpResponse
    */

  def getFailedHttpResponse(statusCode: StatusCode, failedServiceMsg: String ) : HttpResponse = {

    val response = new HttpServicesResponse
    response.getAkkaHttpResponse(statusCode, failedServiceMsg)

  }


   /* *
    * Helper function to extract http request header parameters
    * used for validating the request.
    * @ param httpreq
    * @return (request url, x-vz-date parameter value in header, Authorization value in header)
    *
    * */


  def getAuthorizationElementsFromHttpHeader(httpreq : HttpRequest, authKeys : List[String]) :
                                              Map[String, String] = {

    var requrl = ""
    var reqdateheader = ""
    var reqauthheader = ""
    var authInfo = Map[String, String]()
    try {
      var reqprotocol = HTTPS_PROTOCAL
      if (httpreq.getUri().query().toMap.containsKey("A2T") &&
        httpreq.getUri().query().toMap.get("A2T").equals("2")) {
        reqprotocol = HTTPS_PROTOCAL

      } else if (httpreq.getUri().query().toMap.containsKey("A2T") &&
        !httpreq.getUri().query().toMap.get("A2T").equals("2")) {
        reqprotocol = HTTP_PROTOCOL
      } else {
        reqprotocol = HTTPS_PROTOCAL
      }

      requrl = reqprotocol + httpreq.getHeader("Host").get.value() + httpreq.getUri().path()


      authInfo += (AUTHORIZATION_URL_KEY -> requrl)
      authKeys.foreach( authkey => {
        authInfo += (authkey -> httpreq.getHeader(authkey).get.value())
       }
      )
       logger.info(" request url : " +  httpreq.getUri() +
         " url used for authentication : " + requrl)

    } catch {
      case e: Exception => {
        logger.error(" failed to extract request header elements " , e)
        throw e
      }
    }
    authInfo

  }


   /* *
     * Helper function to extract required authorization
     * from client authorization data.
     * @ param client authorization data
     * @return (values for the keys in authKeys list)
     *
     * */


  def getAuthorizationElementsFromAuthData(clientData : Map[String, String],
                          authKeys : List[String]) : Map[String, String] = {
    var authInfo = Map[String, String]()
    if(clientData != null && authKeys != null ){
    logger.info("fetching " + authKeys.length + " number of authrization elements" +
      " form client data of size : " + clientData.size)
    try {

      authKeys.foreach( authkey => {
        authInfo += (authkey -> clientData.get(authkey).get)
      }
      )

      logger.info("fetched : " + authInfo.size +
        " number of authorization data from client data " )

    } catch {
      case e: Exception => {
        logger.error(" failed to extract request header elements " , e)
        throw e
      }
    }
    }
    authInfo

  }


}
