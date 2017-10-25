package com.verizon.bda.trapezium.framework.kafka.consumer

import java.lang.Long
import java.util

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition

/**
  * Underlying implementation's Message handler to take care of working on the kafka consumer records/messages.
  *
  * @tparam K
  * @tparam V
  */

trait IMessageHandler[K,V]  {
  def handleMessage(records: ConsumerRecords[K, V], begOffsets: util.Map[TopicPartition, Long], untilOffsets: util.Map[TopicPartition, Long], latestOffsets: util.Map[TopicPartition, Long]) : Unit
}
