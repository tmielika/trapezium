package com.verizon.trapezium.api.akkahttp.server

import com.verizon.trapezium.api.akkahttp.routing.AkkaHttpRouteHelper

/**
  * Created by chundch on 5/19/17.
  */
trait AkkaHttpRouteUtil {

  private class ApiSvcsEndpointRoute extends AkkaHttpRouteUtil
   with AkkaHttpRouteHelper

  object AkkaHttpRouteUtil {

    def apply()  {

          val apisvcroute = new ApiSvcsEndpointRoute


     }
  }

}
