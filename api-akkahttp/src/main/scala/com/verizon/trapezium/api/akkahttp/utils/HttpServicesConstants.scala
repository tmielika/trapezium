package com.verizon.trapezium.api.akkahttp.utils

import com.typesafe.config.ConfigFactory

/**
  * Created by chundch on 4/25/17.
  */

object HttpServicesConstants {

  lazy val REQUEST_SIZE_LIMIT = 100 * 1024 * 1024
  lazy val LIVE_STATUS_MSG = "Live!"
  lazy val HTTPS_PROTOCAL = "https://"
  lazy val HTTP_PROTOCOL = "http://"
  lazy val AUTHORIZATION_URL_KEY = "HTTP_AUTHORIZATION_REQ_URL"

  val APP_CONFIGURATION = ConfigFactory.load()
  val HTTP_SERVICES_BINDING_PORT = APP_CONFIGURATION.getInt(
    "apisvcs.akka.http.port")
  lazy val UNAUTHORIZED_ERROR_MSG = APP_CONFIGURATION.getString(
                         "apisvcs.unauthorized.request.message")
  lazy val PAGE_NOT_FOUND_ERROR_MSG = APP_CONFIGURATION.getString(
    "apisvcs.unauthorized.request.message")
  lazy val VZ_DATE_HEADER_KEY = "x-vz-date"
  lazy val VZ_AUTHORIZATION_HEADER_KEY = "Authorization"
  lazy val FAILED_REQUEST_ERROR_MSG = APP_CONFIGURATION.getString(
    "apisvcs.failed.request.message")
  lazy val NO_VALID_AUTHORIZER_ERROR_MSG = APP_CONFIGURATION.getString(
    "apisvcs.failed.noauthorizer.message")

  lazy val FEDERATED_AUTHORIZER_CONFIG = "federated"
  lazy val WSO2_AUTHORIZATION_DATA_KEY = "X-JWT-Assertion"
  lazy val WSO2_AUTHORIZATION_PROVIDER = "wso2"
  lazy val COUCHBASE_AUTHORIZATION_PROVIDER = "couchbase"


}
