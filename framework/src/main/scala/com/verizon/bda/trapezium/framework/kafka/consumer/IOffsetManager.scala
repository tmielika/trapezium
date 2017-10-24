package com.verizon.bda.trapezium.framework.kafka.consumer

/**
  * Offset manager responsible to fetch the offsets of topics partitions for a given topic
  */
trait IOffsetManager {

  def getOffsets(kafkaTopicName: String): Offsets
}

class Offsets(offsets : Map[Int, Long]) {

  def get() : Map[Int, Long] = offsets

}