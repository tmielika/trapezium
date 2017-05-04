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
package com.verizon.bda.trapezium.framework.server.utils

import java.lang.reflect.Constructor

import akka.actor.ActorSystem
import com.verizon.bda.trapezium.framework.server.ServiceEndPoint
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by v468328 on 11/14/16.
  */
object EndPointUtils {
  val logger = LoggerFactory.getLogger(this.getClass)

  def loadEndPoint(endPointClassName: String, sc: SparkContext, as: ActorSystem):
  ServiceEndPoint = {
    val clazz = loadClass(endPointClassName)

    /*
    * Create instance of endpoint using the constructor either the one with having
    * SparkContext param or the one with ActorSystem
    */
    val instance = instanceOf[SparkContext](clazz, classOf[SparkContext], sc) orElse
      instanceOf[ActorSystem](clazz, classOf[ActorSystem], as)

    instance.getOrElse {
      logger.error(
        "EndPoint should have constructors with params of either SparakContext or ActorSystem",
        "Unable to create endpoint instances")
      // Can't do much. Exit with error.
      throw new RuntimeException("Unable to create endpoint instances")
    }
  }

  private def instanceOf[T](clazz: Class[_], paramType: Class[T], param: AnyRef):
  Option[ServiceEndPoint] = {
    val cons = getConstructorOfType(clazz, paramType)
    cons.map(c => c.newInstance(param).asInstanceOf[ServiceEndPoint])
  }


  // scalastyle:off classforname
  def loadClass(name: String): Class[_] = Class.forName(name)

  // scalastyle:on classforname

  def getConstructorOfType[T](clazz: Class[_], paramType: Class[T]): Option[Constructor[_]] =
    Try(clazz.getDeclaredConstructor(paramType)).toOption


}
