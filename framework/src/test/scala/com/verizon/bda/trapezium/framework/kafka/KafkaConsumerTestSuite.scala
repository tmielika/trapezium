/**
  * Copyright (C) 2016 Verizon. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.verizon.bda.trapezium.framework.kafka

import java.util
import java.util.Map.Entry
import java.util.Properties
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.verizon.bda.trapezium.framework.kafka.consumer._
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

/**
  * Tests to ensure that the balanced consumer is working in place.
  * There are threading considerations to be made here for async calls from different threads
  * hence CountDownLatch and AtomicReferences are used.
  */
class KafkaConsumerTestSuite extends KafkaTestSuiteBase {

  val logger = LoggerFactory.getLogger(this.getClass)
  private val DEFAULT_PARTITIONS = 4

  /**
    *
    * Test a simple send receive example
    */
  test("test kafka send-receive") {
    val topicName = "tpSendReceive"
    val (kafkaProducerConfig: Properties, kafkaConsumerConfig: ConsumerConfig) = setupTopicAndConsumerConfig(topicName)
    val messages = 2
    val messagesLatch = new CountDownLatch(messages);
    val offsetsLatch = new CountDownLatch(4);
    val offsetManager = new IOffsetManager {
      override def getOffsets(kafkaTopicName: String): Offsets = {
        //do nothing just catch it.
        logger.info("[send-receive] getOffsets called")
        offsetsLatch.countDown()
        new Offsets(Map())
      }
    }

    val messageHandler = new IMessageHandler[String, String] {
      override def handleMessage(records: ConsumerRecords[String, String], begOffsets: util.Map[TopicPartition, java.lang.Long],
                                 untilOffsets: util.Map[TopicPartition, java.lang.Long],
                                 latestOffsets: util.Map[TopicPartition, java.lang.Long]): Unit = {
        var count = records.count()
        logger.info(s" .. received - ${count} for topic ${topicName}")
        while(count>0) {
          messagesLatch.countDown()
          count = count-1
        }
      }
    }

    val balancedKafkaConsumer = new BalancedKafkaConsumer(kafkaConsumerConfig, offsetManager, messageHandler)
    balancedKafkaConsumer.start()

    offsetsLatch.await(10, TimeUnit.SECONDS)

    val producer = new KafkaProducer[String,String](kafkaProducerConfig)
    logger.info(s"sending messages to kafka topic with partition - ${producer.partitionsFor(topicName)}")

    producer.send(new ProducerRecord[String, String](topicName, "VALUE"))
    producer.send(new ProducerRecord[String, String](topicName, "VALUE"))
    producer.flush()
    producer.close()


    logger.info("[send-receive] Waiting to receive messages")
    messagesLatch.await(10, TimeUnit.SECONDS)
    balancedKafkaConsumer.shutdown()

    assert(messagesLatch.getCount==0,s"Received ${messages-messagesLatch.getCount} messages till now")
  }


  /**
    * Test when multiple consumers are in place and messages are distributed between them
    */
  test("test kafka multiple-consumers") {

    val topiczz = "topic-multiple-consumers"
    val (kafkaProducerConfig: Properties, kafkaConsumerConfig: ConsumerConfig) = setupTopicAndConsumerConfig(topiczz)

    val messageLatch = new CountDownLatch(2);
    val offsetLatch = new CountDownLatch(2);

    val offsetManager1 = new IOffsetManager {
      override def getOffsets(kafkaTopicName: String): Offsets = {
        //do nothing just catch it.
        logger.info("[multiple-consumers] getOffsets called")
        offsetLatch.countDown()
        new Offsets(Map())
      }
    }

    val offsetManager2 = new IOffsetManager {
      override def getOffsets(kafkaTopicName: String): Offsets = {
        //do nothing just catch it.
        logger.info("[multiple-consumers] getOffsets called")
        offsetLatch.countDown()
        new Offsets(Map())
      }
    }

    val messageHandler1 = new IMessageHandler[String, String] {
      override def handleMessage(records: ConsumerRecords[String, String], begOffsets: util.Map[TopicPartition, java.lang.Long],
                                 untilOffsets: util.Map[TopicPartition, java.lang.Long], latestOffsets: util.Map[TopicPartition, java.lang.Long]): Unit = {
        logger.info(s"[multiple-consumers] received - ${records.count()} for topic ${topiczz}")
        var count = records.count()
        while(count>0) {
          messageLatch.countDown()
          count = count-1
        }
      }
    }

    val messageHandler2 = new IMessageHandler[String, String] {
      override def handleMessage(records: ConsumerRecords[String, String]
                                 , begOffsets: util.Map[TopicPartition, java.lang.Long]
                                 , untilOffsets: util.Map[TopicPartition, java.lang.Long]
                                 ,latestOffsets: util.Map[TopicPartition, java.lang.Long]): Unit = {
        logger.info(s"[multiple-consumers] received - ${records.count()} for topic ${topiczz}")
        var count = records.count()
        while(count>0) {
          messageLatch.countDown()
          count = count-1
        }
      }
    }

    val balancedKafkaConsumer1 = new BalancedKafkaConsumer(kafkaConsumerConfig, offsetManager1, messageHandler2);
    balancedKafkaConsumer1.start()

    val balancedKafkaConsumer2 = new BalancedKafkaConsumer(kafkaConsumerConfig, offsetManager2, messageHandler2);
    balancedKafkaConsumer2.start()

    offsetLatch.await(20, TimeUnit.SECONDS)

    val producer = new KafkaProducer[String,String](kafkaProducerConfig)
    producer.send(new ProducerRecord[String, String](topiczz,0,0L, "1", "VALUE-1"))
    producer.send(new ProducerRecord[String, String](topiczz,1,0L, "0", "VALUE-0"))

    logger.info("[multiple-consumers] Waiting to receive messages")
    messageLatch.await(20, TimeUnit.SECONDS)

    assert(messageLatch.getCount==0, "Messages not received")

  }


  /**
    * test when the consumer goes down and the messages are load balanced between the existing consumers
    */
  test("test kafka consumers-switches") {

    val topiczz = "topic-consumers-switches"
    val (kafkaConfig: Properties, kafkaConsumerConfig: ConsumerConfig) = setupTopicAndConsumerConfig(topiczz)

    val messageLatch = new CountDownLatch(8);

    val offsetLatch = new CountDownLatch(4);

    val offsetManager = new IOffsetManager {
      override def getOffsets(kafkaTopicName: String): Offsets = {
        //do nothing just catch it.
        logger.info("[consumers-switches] getOffsets called")
        offsetLatch.countDown()
        new Offsets(Map())
      }
    }

    val messageHandler = new IMessageHandler[String, String] {
      override def handleMessage(records: ConsumerRecords[String, String]
                                 , begOffsets: util.Map[TopicPartition, java.lang.Long]
                                 , untilOffsets: util.Map[TopicPartition, java.lang.Long]
                                 , latestOffsets: util.Map[TopicPartition, java.lang.Long]): Unit = {
        logger.info(s"[consumers-switches] received - ${records.count()} for topic ${topiczz}")
        var count = records.count()
        while(count>0) {
          messageLatch.countDown()
          count = count-1
        }
      }
    }

    val balancedKafkaConsumer1 = new BalancedKafkaConsumer(kafkaConsumerConfig, offsetManager, messageHandler);
    balancedKafkaConsumer1.start()

    val balancedKafkaConsumer2 = new BalancedKafkaConsumer(kafkaConsumerConfig, offsetManager, messageHandler);
    balancedKafkaConsumer2.start()

    offsetLatch.await(10, TimeUnit.SECONDS)

    val producer = new KafkaProducer[String,String](kafkaConfig);
    producer.send(new ProducerRecord[String, String](topiczz,0,System.currentTimeMillis(), "0", "VALUE-1"))
    producer.send(new ProducerRecord[String, String](topiczz,1,System.currentTimeMillis(), "1", "VALUE-0"))
    producer.send(new ProducerRecord[String, String](topiczz,2,System.currentTimeMillis(), "2", "VALUE-1"))
    producer.send(new ProducerRecord[String, String](topiczz,3,System.currentTimeMillis(), "3", "VALUE-0"))
    producer.flush()

    balancedKafkaConsumer2.shutdown();

    logger.info("waiting for rebalance on consumer-1")
    offsetLatch.await(10, TimeUnit.SECONDS)

    producer.send(new ProducerRecord[String, String](topiczz,0,System.currentTimeMillis(), "0", "VALUE-1"))
    producer.send(new ProducerRecord[String, String](topiczz,1,System.currentTimeMillis(), "1", "VALUE-0"))
    producer.send(new ProducerRecord[String, String](topiczz,2,System.currentTimeMillis(), "2", "VALUE-1"))
    producer.send(new ProducerRecord[String, String](topiczz,3,System.currentTimeMillis(), "3", "VALUE-0"))
    producer.flush()

    logger.info("[consumers-switches] Waiting to receive messages")
    messageLatch.await(20, TimeUnit.SECONDS)
    assert(messageLatch.getCount==0, "Messages not received")

  }


  /**
    * Test when the offsets are correctly propagated when the consumers switch and no previously
    * sent messages are sent again.
    */
  test("test kafka consumers-offset-check") {

    val topiczz = "consumers-offset-check"
    val (kafkaConfig: Properties, kafkaConsumerConfig: ConsumerConfig) = setupTopicAndConsumerConfig(topiczz)

    var messageCount = new AtomicInteger();
    val messageLatch = new CountDownLatch(8);

    val offsetLatch = new CountDownLatch(8);

    var untilOffsetReference : AtomicReference[util.Map[TopicPartition, java.lang.Long]] = new AtomicReference[util.Map[TopicPartition, java.lang.Long]]()

    val offsetManager = new IOffsetManager {
      override def getOffsets(kafkaTopicName: String): Offsets = {
        //do nothing just catch it.
        offsetLatch.countDown()
        if(untilOffsetReference==null) {
          logger.info("[consumers-offset-check] getOffsets = empty map")
          new Offsets(Map())
        }
        else {
          var offsetMap:Map[Int, Long] = Map()
          val partitionToLong = untilOffsetReference.get()
          val it = partitionToLong.entrySet().iterator()
          while(it.hasNext) {
            val entry: Entry[TopicPartition, java.lang.Long] = it.next();
            offsetMap +=(entry.getKey.partition() -> entry.getValue)
          }
          logger.info(s"[consumers-offset-check] getOffsets called with ${partitionToLong} for ${offsetMap.mkString(",")} ")
          new Offsets(offsetMap)
        }
      }
    }

    val messageHandler = new IMessageHandler[String, String] {
      override def handleMessage(records: ConsumerRecords[String, String]
                                 , begOffsets: util.Map[TopicPartition, java.lang.Long]
                                 , untilOffsets: util.Map[TopicPartition, java.lang.Long]
                                 , latestOffsets: util.Map[TopicPartition, java.lang.Long]): Unit = {
        if(records.count()>0)
          untilOffsetReference.set(untilOffsets)

        logger.info(s"[consumers-switches]  received - ${records.count()} with offsets ${untilOffsets} for topic ${topiczz}")
        var count = records.count()
        messageCount.addAndGet(count)
        while(count>0) {
          messageLatch.countDown()
          count = count-1
        }
      }
    }

    val balancedKafkaConsumer1 = new BalancedKafkaConsumer(kafkaConsumerConfig, offsetManager, messageHandler);
    balancedKafkaConsumer1.start()

    val balancedKafkaConsumer2 = new BalancedKafkaConsumer(kafkaConsumerConfig, offsetManager, messageHandler);
    balancedKafkaConsumer2.start()

    offsetLatch.await(10, TimeUnit.SECONDS)

    val producer = new KafkaProducer[String,String](kafkaConfig);
    producer.send(new ProducerRecord[String, String](topiczz,0,System.currentTimeMillis(), "0", "VALUE-1"))
    producer.send(new ProducerRecord[String, String](topiczz,1,System.currentTimeMillis(), "1", "VALUE-0"))
    producer.send(new ProducerRecord[String, String](topiczz,2,System.currentTimeMillis(), "2", "VALUE-1"))
    producer.send(new ProducerRecord[String, String](topiczz,3,System.currentTimeMillis(), "3", "VALUE-0"))
    producer.flush()

    balancedKafkaConsumer2.shutdown();

    val balancedKafkaConsumer3 = new BalancedKafkaConsumer(kafkaConsumerConfig, offsetManager, messageHandler);
    balancedKafkaConsumer3.start()

    logger.info("waiting for rebalance on consumer-1")
    offsetLatch.await(10, TimeUnit.SECONDS)

    producer.send(new ProducerRecord[String, String](topiczz,0,System.currentTimeMillis(), "0", "VALUE-1"))
    producer.send(new ProducerRecord[String, String](topiczz,1,System.currentTimeMillis(), "1", "VALUE-0"))
    producer.send(new ProducerRecord[String, String](topiczz,2,System.currentTimeMillis(), "2", "VALUE-1"))
    producer.send(new ProducerRecord[String, String](topiczz,3,System.currentTimeMillis(), "3", "VALUE-0"))
    producer.flush()

    logger.info("[consumers-switches] Waiting to receive messages")
    messageLatch.await(20, TimeUnit.SECONDS)
    assert(messageCount.get()==8, "Correct number of Messages not received")
  }


  private def setupTopicAndConsumerConfig(topiczz: String, partitions: Int=DEFAULT_PARTITIONS, groupId:String ="test") = {
    createTopic(topiczz, partitions)

    val kafkaProducerConfig = getProducerConfig();
    kafkaProducerConfig.put("key.deserializer",classOf[StringDeserializer].getName)
    kafkaProducerConfig.put("value.deserializer",classOf[StringDeserializer].getName)
    kafkaProducerConfig.put("group.id",groupId)

    val topicSet = new java.util.HashSet[String]()
    topicSet.add(topiczz)

    val pollTime = 100
    val waitBetweenPolls = 1000
    val maxRecordSize = 150
    val kafkaConsumerConfig = new ConsumerConfig(kafkaProducerConfig, topicSet, pollTime, waitBetweenPolls, maxRecordSize)

    (getProducerConfig(), kafkaConsumerConfig)
  }
}
