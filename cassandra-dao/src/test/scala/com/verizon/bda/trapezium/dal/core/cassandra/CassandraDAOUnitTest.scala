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
package com.verizon.bda.trapezium.dal.core.cassandra

import com.datastax.driver.core.{ResultSet, Cluster}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.verizon.bda.trapezium.dal.core.cassandra.utils.CassandraDAOUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import scala.collection.JavaConverters._

/**
  * Created by Faraz on 2/29/16.
  * These are unit tests which uses embeded dummy Cassandra.
  * The table used here like iprepuattion or ipreputation are just for testing
  * and should not be confused
  * with tables in netintel keyspace.
  * extends SparkFunSuite with LocalSparkContext
  * @deprecated
  */
class CassandraDAOUnitTest extends CassandraTestSuiteBase {


}
