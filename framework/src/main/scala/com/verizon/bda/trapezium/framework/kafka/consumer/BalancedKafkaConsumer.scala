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
  * if no message handler is specified then its upto clients to make calls to poll and collect
  * the records for a given poll
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

  /**
    * All operations on the consumer is single threaded
    */
  private val executor: ExecutorService = Executors.newSingleThreadExecutor

  private val pollProc = new Callable[Option[PollResult[K, V]]] {

    override def call(): Option[PollResult[K, V]] = {
      var pollResult: PollResult[K, V] = null
      if (!stopped.get()) {
        try {
          pollResult = requestPoll()
        } catch {
          case ex: Exception => {
            logger.error("Failed during poll call.", ex)
          }
        }
      }
      Some(pollResult)
    }
  }

  private val stopProc = new Runnable {

    override def run(): Unit = {

      try {
//       Default closeout time is fine
        consumer.close()
      } catch {
        case ex: Exception => {
          logger.error("Failed to close the consumer.", ex)
        }
      }
      finally {

      }
    }
  }

  private def requestPoll() : PollResult[K,V] = {
    val records = consumer.poll(config.getPollTime())
    logger.info(s"Record count =  ${records.count()}")
    val begOffsets: util.Map[TopicPartition, Long] = new util.HashMap[TopicPartition,Long]()

    /** NOTE:  The API  'consumer.beginningOffsets(records.partitions())' gives the beg offsets with respect to the
      * topic partition offsets saved in server and not from the latest poll call. We have to grab it from the
      * polled messages.
      * Since the messages within the partitions are ordered - take the first item and mark its index as he beginning
      */
    records.partitions().asScala.foreach(parts => {
      val list = records.records(parts)
      if(list!=null && !list.isEmpty)
        begOffsets.put(parts, records.records(parts).get(0).offset())
    })

    val untilOffsets: java.util.Map[TopicPartition, Long] = new util.HashMap[TopicPartition, Long]()
    records.partitions().asScala.par.foreach(tp => {
      try {
        val position = consumer.position(tp)
        untilOffsets.put(tp, position)
      } catch {
        case ex: Exception  =>
          logger.error(s"cannot obtain position for topic partition - ${tp.toString}",ex)
      }
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

  private def startConsumer(): Unit = {

    if (offsetManager == null) {
      logger.warn("No offsetManager is set. The consumer will not support seeking offsets")
      consumer.subscribe(this.config.getTopics())
    }
    else {
      val listener = new ConsumerRebalanceCallback[K, V](offsetManager, consumer)
      consumer.subscribe(this.config.getTopics(), listener)
    }

    if (isMessageTargetDefined) {
      loopingPoll()
    }
  }

  private def isMessageTargetDefined(): Boolean = messageHandler!=null

  /**
    * A simple no-fuss implementation to poll and give call backs
    */
  private def loopingPoll(): Unit = {

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
