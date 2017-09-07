package com.verizon.bda.trapezium.framework.kafka.custom

import org.apache.kafka.common.TopicPartition
import java.util

/**
  * Created by sankma8 on 9/6/17.
  */
class BlockMetadata(batchId: String,
                    beginningOffsets: util.Map[TopicPartition, java.lang.Long],
                    untilOffsets: util.Map[TopicPartition, java.lang.Long],
                    lastOffsets: util.Map[TopicPartition, java.lang.Long])
extends Serializable {

  def getBatchId():String = batchId
  def getBeginningOffsets(): util.Map[TopicPartition, java.lang.Long] = beginningOffsets
  def getUntilOffsets(): util.Map[TopicPartition, java.lang.Long] =  untilOffsets
  def getLastOffsets(): util.Map[TopicPartition, java.lang.Long] = lastOffsets
}
