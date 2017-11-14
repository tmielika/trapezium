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
package com.verizon.bda.trapezium.framework.apps

import java.sql.Time

import com.verizon.bda.trapezium.framework.StreamingTransaction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

/**
 * @author Pankaj on 10/28/15.
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
    logger.info(s" AlgorithmETL: BATCH ${batchID} with ${count}")
    if (batchID == 1 || batchID == 3) require(count == 490, s"Expecting 490 but got ${count} ")
    if (batchID == 2 || batchID == 4) require(count == 499, s"Expecting 499 but got ${count} ")
    batchID += 1
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}
