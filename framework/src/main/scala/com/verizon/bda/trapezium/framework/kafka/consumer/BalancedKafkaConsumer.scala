package com.verizon.bda.trapezium.framework.kafka.consumer


import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect.ClassTag


/**
  * NOTE: Keep this independent of [Spark]
  * A kafka consumer that seeks offsets when the partitions for the current consumer are switched.
  * It uses the message handler to pass on the consumer records of Kafka as a call back to the handler
  *
  * Created by sankma8 on 8/3/17.
  */
class BalancedKafkaConsumer[K:ClassTag ,V: ClassTag](
                                                      val config : ConsumerConfig,
                                                      val offsetManager: IOffsetManager,
                                                      val messageHandler: IMessageHandler[K, V]) extends Serializable {

  lazy val logger = LoggerFactory.getLogger(this.getClass)
  /**
    * This should be a volatile variable to avoid caching problems
    */
  private var stopped  = new AtomicBoolean(false);

  val consumer = new KafkaConsumer[K, V](config.getConsumerProperties())

  var executor: ExecutorService = Executors.newSingleThreadExecutor

  /**
    * not thread safe
    */
  def start(): Unit = {

    if(messageHandler==null)
      throw new IllegalArgumentException("No message handler assigned")

    logger.info(s"starting the consumer on topic - ${config.getTopics()}")

    startConsumer()
  }

  def shutdown(): Unit = {
    logger.info("Shutdown of the receiver initiated ....")
    /**
      * Avoid Consumer shutdown to negate issues with owner thread
      * poll time to await termination
      */
   stopped.lazySet(true)

    try {
      executor.awaitTermination(config.getPollTime() *  3, TimeUnit.MILLISECONDS)
    } finally {
      if(!executor.isShutdown)
        executor.shutdownNow()
    }
  }

  def startConsumer(): Unit = {

    if(offsetManager==null) {
      logger.warn("No offsetManager is set. The consumer will not support seeking offsets")
      consumer.subscribe(this.config.getTopics())
    }
    else {
      val listener = new ConsumerRebalanceCallback[K,V](offsetManager, consumer)
      consumer.subscribe(this.config.getTopics(), listener)
    }

    executor.execute(new Runnable {

      override def run(): Unit = {

        while ( ! stopped.get() ) {
          val records = consumer.poll(config.getPollTime())
          logger.info(s"Record count =  ${records.count()}")
          if( ! stopped.get() )
            try{
              messageHandler.handleMessage(records)
            } catch {
              case ex: Exception => {
                ex.printStackTrace()
              }
          }
        }

        try {
          consumer.close()
        }catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
         finally
         {

        }
      }
    })
  }


  private class ConsumerRebalanceCallback[K,V](offsetManager: IOffsetManager, consumer: KafkaConsumer[K,V]) extends ConsumerRebalanceListener  {

    override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {
      logger.info(s"partitions assigned on the consumer... ${collection.size()}" )

      val iterator = collection.iterator();
      iterator.asScala.foreach(tp => partitionSeek(tp))
    }

    def partitionSeek(topicPartition:TopicPartition): Unit = {
      val partitionOffsets = offsetManager.getOffsets(topicPartition.topic()).get
      val offset: Option[Long] = partitionOffsets.get(topicPartition.partition())

      // when there are offsets managed then propagate them
      if(!offset.isEmpty && offset.get > 0) {
        consumer.seek(topicPartition, offset.get)
        logger.debug(s"Assigned offset= ${offset.get} to partition= ${topicPartition.partition()} for topic ${topicPartition.topic()}" )
      }else{
        logger.debug(s"No offset found for partition= ${topicPartition.partition()} on topic ${topicPartition.topic()}")
      }
    }

    override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
      logger.info("partition revoked on consumer...")
    }
  }
}
