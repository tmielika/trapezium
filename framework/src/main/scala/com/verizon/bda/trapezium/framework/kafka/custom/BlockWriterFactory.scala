package com.verizon.bda.trapezium.framework.kafka.custom

import com.verizon.bda.trapezium.framework.kafka.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Write the messages to the spark block
  * Created by sankma8 on 8/23/17.
  */
object BlockWriterFactory {

  def createDefaultBlockWriter[K: ClassTag, V: ClassTag](consumerConfig: ConsumerConfig,
                                                         writeBlock: (ArrayBuffer[ConsumerRecord[K, V]]) => Unit)
  : IBlockWriter[K, V] = {

    new CountBasedBlockWriter[K,V](consumerConfig,writeBlock)
  }
}

/**
  * A block writer that limits blocks in terms of the number of messages configured
  * @param consumerConfig
  * @param writeBlock
  * @tparam K
  * @tparam V
  */
private class CountBasedBlockWriter[K: ClassTag, V: ClassTag](consumerConfig: ConsumerConfig,
                                                    writeBlock: (ArrayBuffer[ConsumerRecord[K, V]]) => Unit)
  extends IBlockWriter[K, V] {

  var blockCache = scala.collection.mutable.Map[(String, Int), ArrayBuffer[ConsumerRecord[K, V]]]()

  override def init(): Unit = {
    //  nothing to initialize
    blockCache.clear()
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
      val block  = ArrayBuffer[ConsumerRecord[K, V]](consumerRecord)
      blockCache(key) = block
    } else {
      val block: ArrayBuffer[ConsumerRecord[K, V]] = blockCache(key)
      //write back and flush when the threshold is reached
      if (consumerConfig.getMaxRecordCount() > 1  &&  block.size >= consumerConfig.getMaxRecordCount()) {
        flushBlock(block)
      } else {
        blockCache(key) = block.:+(consumerRecord)
      }
    }
  }

  private def flushBlock(block: ArrayBuffer[ConsumerRecord[K, V]]): Unit = {


    if (block.isEmpty)
      return

    writeBlock(block)
    block.clear()
  }

  def complete(): Unit = {
    //      clear the rest of the records accumulated in cache
    blockCache.foreach(records => flushBlock(records._2))
  }

}
