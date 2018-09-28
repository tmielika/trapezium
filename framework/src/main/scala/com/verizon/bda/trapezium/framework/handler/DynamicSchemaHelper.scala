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

package com.verizon.bda.trapezium.framework.handler

import java.io._

import com.typesafe.config.{Config, ConfigFactory}
import com.verizon.bda.trapezium.validation.DataValidator
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

object DynamicSchemaHelper {
  val logger = LoggerFactory.getLogger(this.getClass)

  import org.apache.spark.rdd.RDD

  def generateDataFrame(csv: RDD[Row],
                        name: String,
                        file: String,
                        sparkSession: SparkSession): (String, DataFrame) = {
    var config: Config = null
    var conffilename: String = null
    var conffilecontents: String = null
    logger.info("Input file path:" + file)
    if (file.startsWith("file:")) {
      conffilename = file.stripPrefix("file:") + ".conf"
      logger.info("File path" + conffilename)
      config = ConfigFactory.parseFile(new File(conffilename))
      conffilecontents = FileUtils.readFileToString(new File(conffilename))
    }
    else {

      conffilename = file + ".conf"
      val pt: Path = new Path(conffilename)
      var fs: FileSystem = null
      var br: BufferedReader = null
      try {
        fs = FileSystem.get(new Configuration())
        br = new BufferedReader(new InputStreamReader(fs.open(pt)))
        config = ConfigFactory.parseReader(br)
        val writer: Writer = new StringWriter()
        IOUtils.copy(fs.open(pt), writer, "UTF-8")
        conffilecontents = writer.toString
      }
      finally {
        if (br != null) {
          br.close()
        }
        if (fs != null) {
          fs.close()
        }
      }

    }
    val validator = DataValidator(csv.sparkContext, name)
    logger.info(config.toString)
    logger.info("Config file path:" + conffilename)
    (conffilecontents,
      validator.validateDf(csv, config.getConfig("validation")))
  }
}
