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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

/**
 * Created by Pankaj on 5/11/16.
 */
object KafkaTxn1 extends StreamingTransaction {
  var batchID = 0
  val logger = LoggerFactory.getLogger(this.getClass)
  private val CONST_STRING = "This has to be populated in the preprocess method"
  var populateFromPreprocess: String = _

  override def preprocess(sc: SparkContext): Unit = {
    logger.info("Inside preprocess of AppETL")
    populateFromPreprocess = CONST_STRING
  }

  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    logger.info("Inside ETL")
    val dStream = dStreams.head._2
    require(populateFromPreprocess == CONST_STRING)
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    if (batchID == 0) require(rdd.count() == 490)
    if (batchID == 1) require(rdd.count() == 499)
    require(populateFromPreprocess == CONST_STRING)
    batchID += 1
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}

object KafkaTxn2 extends StreamingTransaction {
  var batchID = 0

  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    val dStream = dStreams.head._2
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    if (batchID == 0 || batchID == 1) require(rdd.count() == 490)
    if (batchID == 2 || batchID == 3) require(rdd.count() == 499)
    batchID += 1
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}

object KafkaTxn3 extends StreamingTransaction {
  var batchID = 0

  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    val dStream = dStreams.head._2
    dStream.print(5)
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    if (batchID == 0 || batchID == 1) require(rdd.count() == 1)
    batchID += 1
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}

object KafkaTxn4 extends StreamingTransaction {
  var batchID = 0
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    val dStream = dStreams.head._2
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    if (batchID < 4) {
      val count = rdd.count()
      require(count == 490 || count == 0)
      logger.info(s"Invoked $batchID times with count $count")
    }
    batchID += 1
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}

object KafkaTxn5 extends StreamingTransaction {
  var batchID = 0
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    val dStream = dStreams.head._2
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    if (batchID < 4) {
      val count = rdd.count()
      logger.info(s"Invoked ${batchID} times with count ${count} [expected = 499 or count 0]")
      require(count == 499 || count == 0)
    }
    batchID += 1
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }

}


/**
  * pass through for persist
  */
object KafkaTxnPassThrough extends StreamingTransaction {

  val logger = LoggerFactory.getLogger(this.getClass)
  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    val dStream = dStreams.head._2
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    logger.info(s"[persistStream] RDD count = ${rdd.count()}")
  }
}
