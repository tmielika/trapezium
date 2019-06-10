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
package com.verizon.bda.trapezium.transformation

import java.io._
import java.util.Properties

import com.typesafe.config.Config
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

import scala.collection.mutable.{Map => MMap}

/**
  * Created by venkatesh on 10/13/16.
  */
object DataTranformer {
  val logger = LoggerFactory.getLogger(this.getClass)
  /**
    * tranformDf takes dataframe read from the source and uses transformation
    * definition from the config file quick map
    *
    * @param df
    * @param batchConfig
    * @return
    */
  def transformDf(df: DataFrame, batchConfig: Config): DataFrame = {
    val columnList = batchConfig.getStringList("transformation.columns")
    val sources = batchConfig.getStringList("transformation.sources")
    val dafaultValues = batchConfig.getStringList("transformation.defaultValues")

    require(columnList.size() == sources.size(), "Number of columns defined in" +
      " transformation block should be same as " +
      "number of sources defined")

    require(columnList.size() == dafaultValues.size(), "Number of columns defined in" +
      " transformation block should be same as " +
      "number of dafaultValues defined")
    var dfTemp = df
    for (i <- 0 until columnList.size()) {
      val source = sources.get(i)
      var map: Map[String, String] = null
      if (source.contains("file://")) {
        map = readPropertiesFromLocal(source)
      } else {
        map = readPropertiesFromHdfs(source, df.sqlContext.sparkContext)
      }
      val default = dafaultValues.get(i)
      dfTemp = populateMapVal(dfTemp, map, columnList.get(i), default)
    }
    dfTemp
  }

  /**
    * populateMapVal will use map values to populate(override)
    * the values of column on dataframe by simple lookup
    *
    * @param df
    * @param map
    * @param column
    * @param default
    * @return
    */
  private[trapezium] def populateMapVal(df: DataFrame, map: Map[String, String],
                                      column: String, default: String): DataFrame = {
    val sc = df.sqlContext.sparkContext
    val broadCastMap: Broadcast[Map[String, String]] = sc.broadcast(map)

    def lookupUdf(map: Map[String, String]) = udf(
      (key: String) => map.getOrElse(key, default)
    )

    val lookupClosure = lookupUdf(broadCastMap.value)

    df.withColumn(column, lookupClosure(col(column)))

  }

  /**
    * method reads properties file hdfs and makes a map out of the file
    *
    * @param source propertiefile on hdfs
    * @param sparkContext
    * @return a map is made from the properties file on hdfs
    */
  private def readPropertiesFromHdfs(source: String,
                                     sparkContext: SparkContext): Map[String, String] = {
    val logger = LoggerFactory.getLogger(this.getClass)
    val path = new Path(source)
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    var map: Map[String, String] = null
    try {
      map = propertiesToMap(fs.open(path))
    } catch {
      case e: Exception =>
        logger.error("error reading source file from hdfs:  " + source, e.getMessage)
    } finally {
      fs.close()
    }
    map
  }

  /**
    * method reads properties file local and makes a map out of the file
    *
    * @param source propertiefile on local
    * @return a map is made from the properties file on local
    */
  private[trapezium] def readPropertiesFromLocal(source: String): Map[String, String] = {
    var input: InputStream = null
    var map: Map[String, String] = null
    try {
      input = new FileInputStream(source)
      map = propertiesToMap(input)
    } catch {
      case e: Exception =>
        logger.error(s"error reading source file from local:   $source {} ", e.getMessage)
    }
    map

  }

  /**
    * takes an InputStream of Properties file and converts to a map
    *
    * @param is
    * @return
    */
  private def propertiesToMap(is: InputStream): Map[String, String] = {
    val isr: InputStreamReader = new InputStreamReader(is, "UTF-8");
    val properties = new Properties
    properties.load(isr)
    is.close()
    properties.asScala.toMap
  }

}
