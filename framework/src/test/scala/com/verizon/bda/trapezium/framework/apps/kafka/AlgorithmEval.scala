/**
  * Copyright (C) 2016 Verizon. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.verizon.bda.trapezium.framework.apps.kafka

import java.sql.Time

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.DStream


/**
  * A custom kafka specific ETL that uses notifications as mechanism for communication
  */
object AlgorithmEval extends BaseStreamingTransaction {

  override def processStream(dStreams: Map[String, DStream[Row]],
                             batchtime: Time): DStream[Row] = {
    logger.info("Inside kafka.Eval")
    val dStream = dStreams.head._2

    dStream.foreachRDD(rdd =>
      logger.info(s"count ${rdd.count()}")
    )

    dStream
  }

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    logger.info(s" kafka.AlgorithmETL: BATCH_ID ${batchID} ")
    super.persistStream(rdd,batchtime)
  }

  override def rollbackStream(batchtime: Time): Unit = {

  }

}

