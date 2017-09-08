package com.verizon.bda.trapezium.framework.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition

import java.lang.Long


import scala.reflect.ClassTag

/**
  * Created by sankma8 on 8/30/17.
  **/

class PollResult[K: ClassTag, V: ClassTag](_records: ConsumerRecords[K, V],
                                           _beginningOffsets: java.util.Map[TopicPartition, Long],
                                           _untilOffsets: java.util.Map[TopicPartition, Long],
                                           _latestOffsets: java.util.Map[TopicPartition, Long]) extends Serializable {
  val records = _records
  val beginningOffsets = _beginningOffsets
  val untilOffsets = _untilOffsets
  val latestOffsets = _latestOffsets

}
