package com.verizon.bda.apisvcs.routing

import java.text.ParseException

import akka.http.scaladsl.model.{HttpRequest, StatusCodes}
import akka.http.scaladsl.server.PathMatchers.Segment
import akka.http.scaladsl.server.{Directives, Route}
import akka.util.ByteString
import com.verizon.bda.apisvcs.ApiHttpServices
import com.verizon.bda.apisvcs.security.{ApiAuthorizationService, FederatedAuthorizerSvc}
import com.verizon.bda.apisvcs.utils.AkkaHttpHeaderUtil
import com.verizon.bda.apisvcs.utils.HttpServicesConstants._
import com.verizon.bda.apisvcs.utils.HttpServicesUtils._
import org.slf4j.LoggerFactory


import scala.collection.mutable.{ListBuffer, Map}

/**
  * Created by chundch on 5/19/17.
  */

trait AkkaHttpRouteHelper extends Directives {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def buildRoute(httpPath: String, routeProcessor : ApiHttpServices) : Route = {
    val publishEndpoint = httpPath
    val routeService = routeProcessor
    val authService = configureAuthorizer(publishEndpoint, routeService)
    val endPointRoute: Route = {

      withSizeLimit(REQUEST_SIZE_LIMIT) {
        post {
          pathPrefix(publishEndpoint) {
            pathEnd {
              logger.info("processing endpoint : " + publishEndpoint)
              complete(getSuccessHttpResponse(
                LIVE_STATUS_MSG))
            } ~
              pathPrefix(Segment) {
                bdaroute: String => {
                  pathEnd {
                    extractRequest { request => {
                      logger.info("processing endpoint : " + bdaroute)
                      handleRequest(request, bdaroute, authService, routeService)
                    }
                    }
                  }
                } ~
                  pathPrefix(Segment) {
                    bdarouteversion: String => {
                      pathEndOrSingleSlash {
                        logger.info("processing bda route : " + bdaroute +
                          " version :" + bdarouteversion)
                        extractRequest {
                          request => {
                            handleRequest(request, bdaroute + "_" + bdarouteversion,
                              authService, routeService)
                          }
                        }
                      }
                    }
                  }
              } ~
              pathEnd {
                logger.info("no path after version ")
                complete(getFailedHttpResponse(
                  StatusCodes.NotFound, PAGE_NOT_FOUND_ERROR_MSG))
              }
          }
        }
      }

    }
    endPointRoute
  }


  /**
    * Helper method to handle api services
    * requests.
    * @param request
    * @param processorId
    * @return Route - akka http route
    */

  def handleRequest(request : HttpRequest, processorId : String,
        authService : ApiAuthorizationService, routeService: ApiHttpServices): Route = {

    logger.info("handleRequest for resource : " + processorId)
    if(authService != null) {

      logger.info("handleRequest authorizing with configured authorizer")
      var validReq : (Boolean, Any) = (false, null)
        try {
          validReq = authService.authorizeApiClient(
          getAuthorizationElementsFromHttpHeader(
            request, authService.authorizationDataAccessKeys()))
        } catch {
          case p: ParseException => {
            logger.error("Failed with parse exception " , p)
            complete(getFailedHttpResponse(StatusCodes.Unauthorized, UNAUTHORIZED_ERROR_MSG))
          }
          case e: Exception => {
            logger.error(" Failed to authenticate " , e)
            complete(getFailedHttpResponse(StatusCodes.Unauthorized, UNAUTHORIZED_ERROR_MSG))
          }
        }
      if (!validReq._1) {
        complete(getFailedHttpResponse(StatusCodes.Unauthorized,
          UNAUTHORIZED_ERROR_MSG))

      } else {

        decodeRequest {
          entity(as[ByteString]) { data: ByteString => {
            try {
              complete(routeService.processServiceRoute(
                getClientProfileFromAuthData( validReq._2,
                routeService.getApiSvcAuthorizer ), processorId, "",
                new AkkaHttpHeaderUtil(request).headerEntries(),
                data.iterator.asInputStream))
            } catch {
              case e: Exception => {
                logger.error(" Error while processing the request ", e)
                complete(getFailedHttpResponse(StatusCodes.BadRequest, FAILED_REQUEST_ERROR_MSG))
              }
            }
          }
          }
        }

      }
    } else {
      complete(getFailedHttpResponse(StatusCodes.InternalServerError,
        NO_VALID_AUTHORIZER_ERROR_MSG))
    }
  }



  /**
    * Helper method to read authorizer configuration
    * and set authorizer for the route.
    * @return ApiAuthorizationService
    */

  def configureAuthorizer(publishEndpoint: String, routeService: ApiHttpServices) :
  ApiAuthorizationService = {
    val routeAuthorizer = routeService.getApiSvcAuthorizer
    var authorizer : ApiAuthorizationService = null
    if (routeAuthorizer.equals( FEDERATED_AUTHORIZER_CONFIG ) ){
      logger.info("setting up route authorizer from configuration : " +  routeAuthorizer)
     // if("samplesvcs".equals(publishEndpoint)) {
        logger.info("setting up route authorizer for samplesvcs")
        authorizer = new FederatedAuthorizerSvc
     // }
    }
//    else {
//      authorizer = new ApiAuthorizationService()
//      authorizer.setAuthSvcProivderType(routeAuthorizer)
//    }
      authorizer

  }

  def getClientProfileFromAuthData(authData : Any, authrizer: String ) : String = {
    var clientProfile: String = ""

    if (authrizer.equals(WSO2_AUTHORIZATION_PROVIDER)) {
      authData.asInstanceOf[Map[String, AnyRef]].toMap.foreach( x => {
        clientProfile += x._1 + ":" + x._2
        logger.debug("claim name : " + x._1 + "claim value : " + x._2)
      })
    } else {
      clientProfile = authData.asInstanceOf[String]
    }
    clientProfile
  }


}
