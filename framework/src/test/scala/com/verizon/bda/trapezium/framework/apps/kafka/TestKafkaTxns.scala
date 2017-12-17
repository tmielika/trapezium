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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

object KafkaTxn1 extends BaseStreamingTransaction {
  private val CONST_STRING = "This has to be populated in the preprocess method"
  var populateFromPreprocess: String = _

  override def preprocess(sc: SparkContext): Unit = {
    logger.info("Inside preprocess of AppETL")
    populateFromPreprocess = CONST_STRING
  }

  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    logger.info("Inside KafkaTxn1 ETL")
    val dStream = dStreams.head._2
    require(populateFromPreprocess == CONST_STRING)
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    super.persistStream(rdd, batchtime)
    require(populateFromPreprocess == CONST_STRING)
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}

object KafkaTxn2 extends BaseStreamingTransaction {


  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    val dStream = dStreams.head._2
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
   super.persistStream(rdd,batchtime)
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}

object KafkaTxn3 extends BaseStreamingTransaction {

  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    val dStream = dStreams.head._2
    dStream.print(5)
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    super.persistStream(rdd,batchtime)
//    if (batchID == 0 || batchID == 1) require(rdd.count() == 1)
//    batchID += 1
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}

object KafkaTxn4 extends BaseStreamingTransaction {
  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    val dStream = dStreams.head._2
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
//    if (batchID < 4) {
      super.persistStream(rdd,batchtime)
      logger.info(s"Invoked $batchID times with count $count")
//    }
    batchID += 1
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }
}

object KafkaTxn5 extends BaseStreamingTransaction {
  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    val dStream = dStreams.head._2
    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
//    if (batchID < 4) {
      super.persistStream(rdd,batchtime)
      logger.info(s"Invoked ${batchID} times with count ${count} [expected = 499 or count 0]")
//      require(count == 499 || count == 0)
//    }
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
