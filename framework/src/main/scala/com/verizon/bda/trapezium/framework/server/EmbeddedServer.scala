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

import java.net.ServerSocket
import javax.servlet.http.HttpServlet

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigObject}
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import org.apache.spark.SparkContext
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by Jegan on 5/20/16.
  */
sealed trait EmbeddedServer {
  val logger = LoggerFactory.getLogger(this.getClass)
  // exposed
  private var bindPort: Int = _

  def getBindPort(): Int = {
    bindPort
  }

  def setBindPort (port: Int): Unit = {

    if (bindPort == 0) {

      logger.info(s"bind port set to $bindPort")
      bindPort = port
    } else {

      logger.warn(s"You can't set bind port again.")

    }
  }

  def init(config: Config)

  def start(config: Config)

  def stop(stopSparkContext: Boolean = false)
}

class AkkaServer extends EmbeddedServer {

  implicit lazy val actorSystem = ActorSystem("AkkaHttpServer")
  implicit lazy val materializer = ActorMaterializer()

  val routes: ListBuffer[Route] = ListBuffer()

  var bindingFuture: Future[Http.ServerBinding] = _

  // Default Exception handler. Endpoints can also define their handlers as part of the route
  // TODO: Do we need to load this class from config so verticals can define their own handlers?
  val exceptionHandler = AkkaHttpExceptionHandler.handler


  // scalastyle:on
  override def init(config: Config): Unit = {
    val routeHandler = new AkkaRouteHandler(null, actorSystem)
    config.getList("endPoints").asScala.foreach(ep => {
      val config = ep.asInstanceOf[ConfigObject].toConfig
      val path = config.getString("path")
      val className = config.getString("className")
      logger.debug(s"Loading the endpoint $className")
      // for every path we need to define  a catchable get and set route.
      // The first route is get and second is set
      val route = routeHandler.defineRoute(path, className)
      routes += route

    })
  }

  override def start(config: Config): Unit = {
    val host = config.getString("hostname")

    val localBindPort =
    // for local as well as jenkins build
      if (ApplicationUtils.env == "local" ){

        val socket = new ServerSocket(0)
        val tempPort = socket.getLocalPort

        // closing the socket
        socket.close()

        tempPort
      } else {

        config.getInt("port")
      }
    logger.info(s"bind port $localBindPort")
    setBindPort(localBindPort)

    // Compose all routes defined by the verticals.
    val route = compose(routes.toList)

    bindingFuture = Http().bindAndHandle(route, host, localBindPort)
  }

  override def stop(stopSparkContext: Boolean = false): Unit = {
    bindingFuture.flatMap(_.unbind())
      .onComplete(_ => actorSystem.shutdown())
  }

  def compose(routes: List[Route]): Route = routes.reduce((r1, r2) => r1 ~ r2)
}

class JettyServer(val serverConfig: Config) extends EmbeddedServer {

  var server: Server = _

  override def init(config: Config): Unit = {
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
    context.setContextPath(serverConfig.getString("contextPath"))

    val localBindPort =
      // for local as well as jenkins build
      if (ApplicationUtils.env == "local" ){

        val socket = new ServerSocket(0)
        val tempPort = socket.getLocalPort

        // closing the socket

        socket.close()
        tempPort
      } else {

        serverConfig.getInt("port")
      }
    logger.info(s"bind port $localBindPort")

    setBindPort(localBindPort)
    server = new Server(localBindPort)

    server.setHandler(context)

    val servletHolders = serverConfig.getList("endPoints")

    servletHolders.asScala.foreach(servletHolder => {

      val servletConfig = servletHolder.asInstanceOf[ConfigObject].toConfig
      // scalastyle:off classforname
      val classInstance = Class.forName(servletConfig.getString("className"))
        .getConstructors()(0).newInstance().asInstanceOf[HttpServlet]

      // scalastyle:on classforname
      context.addServlet(
        new ServletHolder(classInstance), servletConfig.getString("path"))
    })
  }

  override def start(config: Config): Unit = server.start()

  override def stop(stopSparkContext: Boolean = false): Unit = {
    server.stop()
  }
}

