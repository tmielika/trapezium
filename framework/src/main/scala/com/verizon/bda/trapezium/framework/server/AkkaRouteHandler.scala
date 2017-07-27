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
package com.verizon.bda.trapezium.framework.server

import java.lang.Exception

import akka.actor.ActorSystem
import akka.http.javadsl.model.ResponseEntity
import akka.http.javadsl.server.RequestContext
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpHeader, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import com.verizon.bda.trapezium.framework.server.utils.{AkkaRouterStartUp, EndPointUtils}
import com.verizon.bda.trapezium.framework.utils.ReflectionSupport
import org.apache.spark.SparkContext

import scala.concurrent.Future


/**
  * Created by Jegan on 5/24/16.
  */
class AkkaRouteHandler(sc: SparkContext, implicit val as: ActorSystem) extends ReflectionSupport {


  val exceptionHandler = AkkaHttpExceptionHandler.handler
  AkkaRouterStartUp.loadConfig()
  def defineRoute(path: String, endPointClassName: String): Route = {
    require(path != null && !path.isEmpty, "Path cannot be null or empty")
    require(endPointClassName != null && !endPointClassName.isEmpty,
      "End-point class name cannot be null or empty")

    val endPoint: ServiceEndPoint = EndPointUtils.loadEndPoint(endPointClassName, sc, as)

    implicit val materializer = ActorMaterializer()
    pathPrefix(path) {
      handleExceptions(exceptionHandler) {
        endPoint.route
      }

    }

  }
}



