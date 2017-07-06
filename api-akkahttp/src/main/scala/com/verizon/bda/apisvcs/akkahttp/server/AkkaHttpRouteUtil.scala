package com.verizon.bda.apisvcs.akkahttp.server

import com.verizon.bda.apisvcs.ApiHttpServices
import com.verizon.bda.apisvcs.routing.{AkkaHttpRouteHelper}

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
