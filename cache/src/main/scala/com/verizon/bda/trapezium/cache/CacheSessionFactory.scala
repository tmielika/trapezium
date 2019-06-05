/**
  * Copyright (C) 2016 Verizon. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.verizon.bda.trapezium.cache

import java.io.IOException
import java.lang.reflect.Constructor
import java.net.{URI, InetSocketAddress, URL}
import com.typesafe.config.ConfigFactory
import com.verizon.bda.trapezium.dal.data.KVStore
import scala.collection.mutable
import scala.util.Try
import org.slf4j.LoggerFactory

/**
  * Created by v468328 on 9/22/16.
  * Cache Session Factory. It creates reference cache for application as per
  * implementation specified in property commons.cache.impl.
  * For CouchBase we need corresponding buckets in CouchBase.
  *
  */

class CacheSessionFactory[K, V] {
  val logger = LoggerFactory.getLogger(this.getClass)
  private var cacheMap: scala.collection.mutable.Map[String, KVStore[K, V]] =
    new mutable.HashMap[String, KVStore[K, V]]()
  private val globalBucket = CacheConfig.config.getString("default.app.name")
  private val retryMax = CacheConfig.config.getInt("commons.cache.retryCount")
  private val retryDelay = CacheConfig.config.getInt("commons.cache.retryInterval")
  private var cacheImplClassName = CacheConfig.config.getString("commons.cache.impl")


  /**
    * Returns Application specfic bucket.
    *
    * @param appName This translate to app specfic bucket in case of couch Base
    * @param pwd     The password for couchBase
    * @return
    */
  def getCache(appName: String = globalBucket, pwd: String , retryCount: Int = 0):
  KVStore[K, V] = {

    logger.debug("params to init cache are default app name => " + globalBucket +
      " cacheImplClassName => " + cacheImplClassName)

    if (cacheMap.get(appName) == None) {
      // scalastyle:off classforname
      val cacheClass = Class.forName(cacheImplClassName)
      // scalastyle:on classforname
      val cacheObject = cacheClass.newInstance()
      val cache = cacheObject.asInstanceOf[KVStore[K, V]]


      try {
        cache.init(appName, pwd)

      } catch {
        case ioe: IOException => {

          if (retryCount < retryMax) {
            Thread.sleep(retryDelay)
            return getCache(appName, pwd, retryCount + 1)
          }
          else {
            logger.error(s"Unable to initialize the app cache > $appName" +
              s" for class -> $cacheImplClassName {}", ioe.getMessage)
          }

        }
      }
      cacheMap.put(appName, cache)
    }

    return cacheMap.get(appName).get

  }

}
