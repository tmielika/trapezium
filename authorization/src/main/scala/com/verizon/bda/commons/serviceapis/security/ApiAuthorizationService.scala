package com.verizon.bda.commons.serviceapis.security

import org.slf4j.LoggerFactory


/**
  * Created by chundch on 5/1/17.
  */

trait ApiAuthorizationService {

  private val logger = LoggerFactory.getLogger(this.getClass)


  def getAuthSvcProivderType(): String

  def setAuthSvcProivderType(provider: String): Unit = {
  }

  def authorizationDataAccessKeys(): List[String] = {
    Nil
  }

  def authorizeApiClient(authData: Map[String, String]): (Boolean, Any)


}
