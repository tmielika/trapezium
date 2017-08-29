package com.verizon.trapezium.api.utils

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

  lazy val SAMPLE_SVCS_DATE_HEADER_KEY = "x-vz-date"
  lazy val SAMPLE_SVCS_AUTHORIZATION_HEADER_KEY = "Authorization"


  val SAMPLE_HTTP_SERVICE_RESOURCE = APP_CONFIGURATION.getString(
    "apisvcs.endpoint.examplesvcs.resource")

}
