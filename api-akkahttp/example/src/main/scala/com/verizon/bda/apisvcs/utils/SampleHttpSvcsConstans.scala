package com.verizon.bda.apisvcs.utils

import com.typesafe.config.ConfigFactory

/**
  * Created by chundch on 5/9/17.
  */
object SampleHttpSvcsConstans {


  val APP_CONFIGURATION = ConfigFactory.load()
  val EXAMPLE_SVCS_ROUTE_AUTHORIZER = APP_CONFIGURATION.getString(
    "apisvcs.examplesvcs.route.authorizer")
  val SAMPLE_HTTP_SERVICE_PUBLISH_ENDPOINT = APP_CONFIGURATION.getString(
    "apisvcs.examplesvcs.endpoint.topublish")

  val SAMPLE_HTTP_SERVICE_BINDING_PORT = APP_CONFIGURATION.getInt(
    "apisvcs.examplesvcs.bindingport")

  lazy val WSO2_AUTHORIZATION_DATA_KEY = "X-JWT-Assertion"

  val SAMPLE_HTTP_SERVICE_RESOURCE = APP_CONFIGURATION.getString(
    "apisvcs.endpoint.examplesvcs.resource")

}
