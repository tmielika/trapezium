package com.verizon.bda.trapezium.framework.kafka.ha

import java.lang.Long
import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

/**
  * An interface contract for writing consumer records
 * Created by sankma8 on 8/22/17.
 */
trait IBlockWriter[K, V] {

  def init(begOffsets: util.Map[TopicPartition, Long], untilOffsets: util.Map[TopicPartition, Long], latestOffsets: util.Map[TopicPartition, Long])

  def store(consumerRecord : ConsumerRecord[K,V]) : Unit
  def complete(): Unit
}
