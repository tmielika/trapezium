package com.verizon.bda.trapezium.framework.kafka.custom

import java.lang.Long
import java.util

import com.verizon.bda.trapezium.framework.kafka.consumer.IMessageHandler
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * Handles the consumer records and parallelizes consumption on a partition basis
  */
class MessageHandler[K: ClassTag, V: ClassTag](blockWriter : IBlockWriter[K,V] ) extends IMessageHandler[K, V] {

  lazy val logger = LoggerFactory.getLogger(this.getClass)

  override def handleMessage(consumerRecords: ConsumerRecords[K, V],
                             begOffsets: util.Map[TopicPartition, Long],
                             untilOffsets: util.Map[TopicPartition, Long],
                             latestOffsets: util.Map[TopicPartition, Long]): Unit = {

    if(consumerRecords.count() == 0)
      return

    blockWriter.init()

    val partitions = consumerRecords.partitions()
    val iterator = partitions.iterator().asScala

    //Parallelize processing messages based on the number of partitions
    iterator.toStream.par.foreach(topicPartition => storeRecords(consumerRecords.records(topicPartition)))

    blockWriter.complete()
  }

  // retain order within a partition
  def storeRecords(records: util.List[ConsumerRecord[K, V]]): Unit = {
    logger.info(s"Received messages = ${records.size()}")
    records.iterator().asScala.foreach( record => blockWriter.store(record))
  }

}