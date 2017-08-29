package com.verizon.bda.trapezium.framework.kafka.custom

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * An interface contract for writing consumer records
 * Created by sankma8 on 8/22/17.
 */
trait IBlockWriter[K, V] {
  def init() : Unit
  def store(consumerRecord : ConsumerRecord[K,V]) : Unit
  def complete(): Unit
}
