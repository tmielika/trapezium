package com.verizon.bda.commons.serviceapis.security.utils

import com.verizon.bda.commons.serviceapis.security.{ApiAuthorizationService, FederatedAuthorizerSvc}
import AuthorizationServicesConstants._
import org.slf4j.LoggerFactory

object AuthorizationHelper {
  private val logger = LoggerFactory.getLogger(this.getClass)

  @throws
  def getAuthorizer(authConstant: String): ApiAuthorizationService = {

    if (authConstant.equals(FEDERATED_AUTHORIZER_CONFIG)) {
      logger.info("setting up route authorizer from configuration : " + authConstant)
      // if("samplesvcs".equals(publishEndpoint)) {
      logger.info("setting up route authorizer for samplesvcs")
      return new FederatedAuthorizerSvc
    }
    else {
      logger.error(s"invalid authorizer configured for $authConstant")
      throw new Exception("auth constant is not valid")
    }
  }
}
