package com.verizon.bda.trapezium.framework.kafka.consumer


import java.lang.Long
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
class BalancedKafkaConsumer[K: ClassTag, V: ClassTag](
                                                       val config: ConsumerConfig,
                                                       val offsetManager: IOffsetManager,
                                                       val messageHandler: IMessageHandler[K, V]
                                                       ) extends Serializable {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * This is shared across threads so consistency is important
    */
  private val stopped = new AtomicBoolean(false);

  private val consumer = new KafkaConsumer[K, V](config.getConsumerProperties())

  private val executor: ExecutorService = Executors.newSingleThreadExecutor

  private val pollProc = new Callable[Option[PollResult[K, V]]] {

    override def call(): Option[PollResult[K, V]] = {
      var pollResult: PollResult[K, V] = null
      if (!stopped.get()) {
        try {
          pollResult = requestPoll()
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
      Some(pollResult)
    }
  }

  private val stopProc = new Runnable {

    override def run(): Unit = {

      try {
        //          Default closeout time is fine
        consumer.close()
      } catch {
        case ex: Exception => {

          ex.printStackTrace()
        }
      }
      finally {

      }
    }
  }

  private def requestPoll() : PollResult[K,V] = {
    val records = consumer.poll(config.getPollTime())
    logger.info(s"Record count =  ${records.count()}")
    val begOffsets = consumer.beginningOffsets(records.partitions())
    val untilOffsets: java.util.Map[TopicPartition, Long] = new util.HashMap[TopicPartition, Long]()
    records.partitions().asScala.par.foreach(tp => {
      untilOffsets.put(tp, consumer.position(tp))
    })
    val endOffsets = consumer.endOffsets(records.partitions())
    return new PollResult(records, begOffsets, untilOffsets, endOffsets)
  }

  /**
    * not thread safe
    */
  def start(): Unit = {

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
      if (!isMessageTargetDefined)
        executor.submit(stopProc)

      executor.awaitTermination(config.getPollTime() * 3, TimeUnit.MILLISECONDS)

    } finally {
      if (!executor.isShutdown)
        executor.shutdownNow()
    }
  }

  def poll(): Option[PollResult[K, V]] = {
    if (isMessageTargetDefined)
      throw new IllegalAccessException("The current object is configured with continuous poll instead of discrete poll calls ")
    val future = executor.submit(pollProc)
    val maybeResult = future.get()
    return maybeResult
  }

  def startConsumer(): Unit = {

    if (offsetManager == null) {
      logger.warn("No offsetManager is set. The consumer will not support seeking offsets")
      consumer.subscribe(this.config.getTopics())
    }
    else {
      val listener = new ConsumerRebalanceCallback[K, V](offsetManager, consumer)
      consumer.subscribe(this.config.getTopics(), listener)
    }

    if (isMessageTargetDefined) {
/*
      val sesPool = Executors.newSingleThreadScheduledExecutor()
      sesPool.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          while (!stopped.get() && ! executor.isShutdown) {
            executor.submit(pollProc)
          }

        }},0, 10, TimeUnit.MILLISECONDS)
*/
      loopingPoll()
    }
  }

  def isMessageTargetDefined(): Boolean = messageHandler!=null

  def loopingPoll(): Unit = {

    executor.execute(new Runnable {

      override def run(): Unit = {

        while (!stopped.get()) {
          if (!stopped.get())
          try {
             val pollResult = requestPoll()
              messageHandler.handleMessage(pollResult.records, pollResult.beginningOffsets, pollResult.untilOffsets, pollResult.latestOffsets)
            } catch {
              case ex: Exception => {
                logger.error("Unable to publish message to message handler", ex)
              }
            }
        }

        try {
          //          Default closeout time is fine
          consumer.close()
        } catch {
          case ex: Exception => {
            logger.error("problems closing the consumer", ex)
          }
        }
        finally {

        }
      }
    })
  }


  private class ConsumerRebalanceCallback[K, V](offsetManager: IOffsetManager, consumer: KafkaConsumer[K, V]) extends ConsumerRebalanceListener {

    override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {
      logger.info(s"partitions assigned on the consumer... ${collection.size()}")

      val iterator = collection.iterator();
      iterator.asScala.foreach(tp => partitionSeek(tp))
    }

    def partitionSeek(topicPartition: TopicPartition): Unit = {
      if (offsetManager == null)
        return

      val partitionOffsets = offsetManager.getOffsets(topicPartition.topic()).get

      if (partitionOffsets == null || partitionOffsets.isEmpty)
        return

      val offset = partitionOffsets.get(topicPartition.partition())

      // when there are offsets managed then propagate them
      if (!offset.isEmpty && offset.get > 0) {
        consumer.seek(topicPartition, offset.get)
        logger.debug(s"Assigned offset= ${offset.get} to partition= ${topicPartition.partition()} for topic ${topicPartition.topic()}")
      } else {
        logger.debug(s"No offset found for partition= ${topicPartition.partition()} on topic ${topicPartition.topic()}")
      }
    }

    override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
      logger.info("partition revoked on consumer...")
    }
  }

}
