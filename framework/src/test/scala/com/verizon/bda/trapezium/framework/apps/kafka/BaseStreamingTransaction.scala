package com.verizon.bda.trapezium.framework.apps.kafka

import java.sql.Time

import com.verizon.bda.trapezium.framework.StreamingTransaction
import com.verizon.bda.trapezium.framework.apps.{STAGE, TestConditionManager, TestEventFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

/**
  * Created by sankma8 on 11/14/17.
  */
abstract class BaseStreamingTransaction extends StreamingTransaction {
  var batchID = 0
  val logger = LoggerFactory.getLogger(this.getClass)
  var count = 0L

  override def persistStream(rdd: RDD[Row], batchtime: Time): Unit = {
    count = rdd.count
    logger.info(s" ${getClass.getSimpleName}: BATCH_ID ${batchID} with ${count}")
    val condition = TestEventFactory.createTestEventMap(getClass.getSimpleName, STAGE.persistStream, batchID, count)
    TestConditionManager.notify(condition)
    batchID += 1
  }

}

