package com.verizon.bda.trapezium.framework.kafka.ha

import java.lang.Long
import java.util
import java.util.{Collections, UUID}

import com.verizon.bda.trapezium.framework.kafka.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Write the messages to the spark block
  * Created by sankma8 on 8/23/17.
  */
object BlockWriterFactory {

  def createDefaultBlockWriter[K: ClassTag, V: ClassTag](consumerConfig: ConsumerConfig,
                                                         writeBlock: (ArrayBuffer[ConsumerRecord[K, V]], BlockMetadata) => Unit)
  : IBlockWriter[K, V] = {

    new PartitionAndCountBasedBlockWriter[K, V](consumerConfig, writeBlock)
  }
}

/**
  * A block writer that limits blocks in terms of the number of messages configured. Assumes that the
  * block contains messages from one partition
  *
  * @param consumerConfig
  * @param writeBlock
  * @tparam K
  * @tparam V
  */
private class PartitionAndCountBasedBlockWriter[K: ClassTag, V: ClassTag](consumerConfig: ConsumerConfig,
                                                              writeBlock: (ArrayBuffer[ConsumerRecord[K, V]], BlockMetadata) => Unit)
  extends IBlockWriter[K, V] {


  private val logger = LoggerFactory.getLogger(this.getClass)

  var blockCache = scala.collection.mutable.Map[(String, Int), ArrayBuffer[ConsumerRecord[K, V]]]()
  var begOffsets: util.Map[TopicPartition, Long] = Collections.emptyMap()
  var untilOffsets: util.Map[TopicPartition, Long] = Collections.emptyMap()
  var latestOffsets: util.Map[TopicPartition, Long] = Collections.emptyMap()
  var id: UUID = _

  def init(begOffsets: util.Map[TopicPartition, Long],
           untilOffsets: util.Map[TopicPartition, Long],
           latestOffsets: util.Map[TopicPartition, Long]): Unit = {
    id = UUID.randomUUID();
    blockCache.clear()
    this.begOffsets = begOffsets
    this.untilOffsets = untilOffsets
    this.latestOffsets = latestOffsets

  }


  /**
    * adds a consumer record to the block manager. Each block represents a partition in a given topic
    *
    * @param consumerRecord
    */
  def store(consumerRecord: ConsumerRecord[K, V]): Unit = {

    val key = (consumerRecord.topic(), consumerRecord.partition()) // For a block, topic + partition is unique and is the key

    val partitionedRecords = blockCache.get(key)

    if (partitionedRecords.isEmpty) {
      val block = ArrayBuffer[ConsumerRecord[K, V]](consumerRecord)
      blockCache.put(key, block)
    } else {
      var block: ArrayBuffer[ConsumerRecord[K, V]] = blockCache(key)
      block = block.:+(consumerRecord)
      blockCache(key) =  block
//      write back and flush when the threshold is reached
      if (consumerConfig.getMaxRecordCount() > 1 && block.size >= consumerConfig.getMaxRecordCount()) {
        flushBlock(false, key, block)
      }
    }
  }

  private def flushBlock(isLastSegment: Boolean, key: (String, Int), block: ArrayBuffer[ConsumerRecord[K, V]]): Unit = {

    logger.info(s"Segment ${isLastSegment}. Flushing the block - ${block.size}")

    /**
      * Messages in a block are per partition so we can store the partition details for end blocks
      * These serve as 'watermark' that keeps the last location of the block
      */
    if (isLastSegment) {
      val partition = new TopicPartition(key._1, key._2)
      val _begOffsets: util.Map[TopicPartition, Long] = new util.HashMap[TopicPartition, Long]()
      _begOffsets.put(partition, this.begOffsets.get(partition))

      val _untilOffsets: util.Map[TopicPartition, Long] = new util.HashMap[TopicPartition, Long]()
      _untilOffsets.put(partition, this.untilOffsets.get(partition))

      val _latestOffsets: util.Map[TopicPartition, Long] = new util.HashMap[TopicPartition, Long]()
      _latestOffsets.put(partition, this.latestOffsets.get(partition))

      val blockMetadata = new BlockMetadata(id.toString, _begOffsets, _untilOffsets, _latestOffsets, key._1, key._2)
      writeBlock(block, blockMetadata)
    }

    else {
      writeBlock(block, null)
    }


    block.clear()
  }

  def complete(): Unit = {
    //      flush rest of the records accumulated in cache - if no records then empty set will be sent for flushBlock
    blockCache.foreach(records => flushBlock(true, records._1, records._2))

    //    Clear the state
    this.begOffsets = Collections.emptyMap()
    this.untilOffsets = Collections.emptyMap()
    this.latestOffsets = Collections.emptyMap()
  }
}