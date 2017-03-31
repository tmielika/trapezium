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

import java.io.File

import com.typesafe.config.{ConfigFactory, Config}
import com.verizon.bda.trapezium.cache.CacheConfig
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import com.verizon.bda.trapezium.framework.{ApplicationManager, ApplicationManagerStartup}
import org.slf4j.LoggerFactory

/**
  * Created by v468328 on 11/23/16.
  */
object AkkaRouterStartUp {


  def loadConfig(): Unit = {


    val applicationConfig: ApplicationConfig = ApplicationManager.getConfig()
    val logger = LoggerFactory.getLogger(this.getClass)

    logger.debug("We are reading config inside AkkaRouterStartUp env= "
      +  applicationConfig.getEnv() + " config dir =>  " + applicationConfig.getConfigDir())

    logger.debug("The config file is => " +
      applicationConfig.getConfigDir() + "/" +  applicationConfig.getEnv()  + "_cache.conf")

    if (applicationConfig.getConfigDir() != null) {
      logger.info(s"Reading configuration files from ${applicationConfig.getConfigDir()}")
    }
    try {
      var env = applicationConfig.getEnv()
      if (env == null) {
        env = "local"
      }
      logger.
        info(s"Application properties path: ${applicationConfig.getConfigDir()}/${env}_cache.conf")
      if (applicationConfig.getConfigDir() == null) {
        CacheConfig.loadConfigFromResource(s"${env}_cache.conf")
      }
      else {

        CacheConfig.loadConfigFromFile(applicationConfig.getConfigDir()
          +  "/" +  applicationConfig.getEnv()  + "_cache.conf")
      }


    } catch {
      case ex: NullPointerException => logger.error("Property file not found",
        ex.getMessage)
        sys.exit()
    }
  }

}
