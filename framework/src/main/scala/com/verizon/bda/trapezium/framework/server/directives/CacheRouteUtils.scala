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

import java.lang.reflect.Constructor

import akka.actor.ActorSystem
import akka.http.scaladsl.server.RouteResult
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.verizon.bda.trapezium.cache.CommonCacheSessionFactory
import com.verizon.bda.trapezium.framework.server.ServiceEndPoint
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

/**
  * Created by v468328 on 10/25/16.
  * It contains utility/helper code for cache directive
  * the purpose here  is to not clutter directive code
  */
object CacheRouteUtils {
  val logger = LoggerFactory.getLogger(this.getClass)
  val CACHE_ID: String = "cache-key"
  val NO_CACHE: String = "no-store"
  val CACHED: String = "cached"
  val CONTENT_TYPE = "Content-Type"
  // meaning this response is coming from cache.
  val factory: CommonCacheSessionFactory[String, String] =
    new CommonCacheSessionFactory[String, String]

  val cache = factory.getCache()

  def put(response: Future[String], cacheId: String): Unit = {

    response.onSuccess { case result => cache.put(cacheId, result) }

    response.onFailure { case exception => logger.error("exception occured while saving " +
      "to cache cache-id " + cacheId, exception.getMessage)
    }

  }

  def put(cacheId: String, responseFuture: Future[RouteResult], materializer_p: ActorMaterializer) :

  Unit = {

    implicit val materializer: ActorMaterializer = materializer_p
    responseFuture.onSuccess { case result =>

      if (result.isInstanceOf[RouteResult.Complete]) {
        val httpresponse: RouteResult.Complete = result.asInstanceOf[RouteResult.Complete]
        val unmarshalledResponse: Future[String] =
          Unmarshal(httpresponse.response.entity).to[String]
        unmarshalledResponse.onSuccess {
          case finalResponse => cache.put(cacheId, finalResponse)
        }
      }
      else if (result.isInstanceOf[RouteResult.Rejected]) {

        val httpresponse: RouteResult.Rejected = result.asInstanceOf[RouteResult.Rejected]


      }
    }

    responseFuture.onFailure { case exception => {
      logger.error("exception happened ",
        exception.getMessage)
    }

    }

  }


  def get(cacheId: String): Option[String] = {

    val result = cache.get(cacheId)
    result
  }


}
