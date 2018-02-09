package com.verizon.bda.commons.serviceapis.security

import org.slf4j.LoggerFactory
import com.verizon.bda.commons.serviceapis.security.utils.AuthSvcsConstants._

/**
  * Created by chundch on 5/9/17.
  */
class FederatedAuthorizerSvc extends ApiAuthorizationService {

  private val logger = LoggerFactory.getLogger(this.getClass)
  val authSvcProviderType = "Federated"

  override def authorizeApiClient(authData: Map[String, String]): (Boolean, Any) = {

    logger.info("authorization data map size : " + (if (authData != null) authData.size else 0))

    // @TODO add support for external authenticators

    (true, "")

  }

  override def authorizationDataAccessKeys(): List[String] = {

    val keysList = List(FEDERATED_AUTH_TYPE_HEADER_KEY, FEDERATED_AUTH_TOKEN)
    logger.info("authorization data access keys size : " + keysList.size)

    keysList
  }

  override def getAuthSvcProivderType(): String = {
    authSvcProviderType
  }

}
