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
package com.verizon.bda.trapezium.framework.apps.kafka

import java.sql.Time

import com.verizon.bda.trapezium.framework.StreamingTransaction
import com.verizon.bda.trapezium.framework.apps.{TestConditionFactory, TestConditionManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

/**
 * A custom kafka specific ETL that uses notifications as mechanism for communication
 */
object AlgorithmETL extends StreamingTransaction {
  var batchID = 0
  val logger = LoggerFactory.getLogger(this.getClass)

  override def processStream(dStreams: Map[String, DStream[Row]],
                       batchtime: Time): DStream[Row] = {
    val dStream = dStreams.head._2
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    val count = rdd.count
    logger.info(s" kafa.AlgorithmETL: BATCH ${batchID} with ${count}")
    val condition = TestConditionFactory.createCondition(getClass.getSimpleName, "persistSchema", batchID, count)
    TestConditionManager.notify(condition)
//    if (batchID == 1 || batchID == 3) require(rdd.count() == 490, s"Expecting 490 but got ${rdd.count()} ")
//    if (batchID == 2 || batchID == 4) require(rdd.count() == 499, s"Expecting 499 but got ${rdd.count()} ")
    batchID += 1
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}
