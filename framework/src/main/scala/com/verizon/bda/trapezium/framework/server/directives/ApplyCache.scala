/**
* Copyright (C) 2016 Verizon. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.verizon.bda.trapezium.framework.server.directives

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.verizon.bda.trapezium.cache.CacheConfig
import com.verizon.bda.trapezium.framework.server.ServiceEndPoint
import org.apache.spark.SparkContext
import scala.concurrent.Future

/**
  * Created by  v468328 on 10/25/16.
  */
class ApplyCache() {


  def apply(ctx: akka.http.scaladsl.server.RequestContext, endPoint: ServiceEndPoint,
            sc: SparkContext, materializer: ActorMaterializer):
  Future[akka.http.scaladsl.server.RouteResult]
  = {


    val cache_turnoff_switch = CacheConfig.config.getBoolean("cache_turnoff_switch")

    var uri_path = ""
    if (ctx.request.getUri() != null) {
      uri_path = ctx.request.getUri().path();
    }

    // First we should check for master turn on or off switch.
    // if turn out then just process inner route and bypass

    if (cache_turnoff_switch) {

      return endPoint.route(ctx)

    }
    val cacheId = ctx.request.getHeader(CacheRouteUtils.CACHE_ID)
    val no_cache_Directive = ctx.request.getHeader(CacheRouteUtils.NO_CACHE)

    // no-cache is standrad dire-ctive to avoid cache.
    // val content_type = ctx.request.getHeader(CacheRouteUtils.CONTENT_TYPE)

    var response: Option[String] = None;
    if (cacheId.isDefined && no_cache_Directive.isEmpty) {
      response = CacheRouteUtils.get(uri_path + cacheId.get.value())

    }

    if (response.isDefined) {
      // set headers
      val userData = ByteString(response.get)
      // logger.info("reponse from cache " + userData)

      ctx.complete(HttpEntity(ContentTypes.`application/json`, userData))


    }
    else {

      val resultFuture: Future[akka.http.scaladsl.server.RouteResult] = endPoint.route(ctx)
      if (cacheId.isDefined && no_cache_Directive.isEmpty) {
        // only if we want to cache it.
        CacheRouteUtils.put(uri_path + cacheId.get.value(), resultFuture, materializer)
      }

      resultFuture
    }
  }


}
