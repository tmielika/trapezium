/**
* Copyright (C) 2016 Verizon. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file is partially imported from Spark source as it provides
 * Kafka client utilities that we otherwise need to rewrite. However,
 * we could not use it directly from the kafka library, as it is declared
 * private within the "spark" streaming package. The original file is modified
 * to retain only the required functionality and the rest is dropped.
 *
 * Please retain the license statement above as a file header at all times.
 */

package com.verizon.bda.trapezium.framework.kafka

import scala.util.control.NonFatal
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import java.util.Properties

import kafka.api._
import kafka.common.{ErrorMapping, OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import org.apache.kafka.common.TopicPartition
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import org.apache.spark.SparkException

/**
 * Convenience methods for interacting with a Kafka cluster.
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 * configuration parameters</a>.
 *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
 *   NOT zookeeper servers, specified in host1:port1,host2:port2 form
 */
// private[kafka]
class KafkaCluster(val kafkaParams: Map[String, String]) extends Serializable {
  import KafkaCluster.{Err, LeaderOffset, SimpleConsumerConfig}

  // ConsumerConfig isn't serializable
  @transient private var _config: SimpleConsumerConfig = null

  def config: SimpleConsumerConfig = this.synchronized {
    if (_config == null) {
      _config = SimpleConsumerConfig(kafkaParams)
    }
    _config
  }

  def connect(host: String, port: Int): SimpleConsumer =
    new SimpleConsumer(host, port, config.socketTimeoutMs,
      config.socketReceiveBufferBytes, config.clientId)

  def connectLeader(topic: String, partition: Int): Either[Err, SimpleConsumer] =
    findLeader(topic, partition).right.map(hp => connect(hp._1, hp._2))

  // Metadata api
  // scalastyle:off
  // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
  // scalastyle:on

  def findLeader(topic: String, partition: Int): Either[Err, (String, Int)] = {
    val req = TopicMetadataRequest(TopicMetadataRequest.CurrentVersion,
      0, config.clientId, Seq(topic))
    val errs = new Err
    withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
      val resp: TopicMetadataResponse = consumer.send(req)
      resp.topicsMetadata.find(_.topic == topic).flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.find(_.partitionId == partition)
      }.foreach { pm: PartitionMetadata =>
        pm.leader.foreach { leader =>
          return Right((leader.host, leader.port))
        }
      }
    }
    Left( errs )
  }

  def findLeaders(
                   TopicPartitions: Set[TopicAndPartition]
                   ): Either[Err, Map[TopicAndPartition, (String, Int)]] = {
    val topics = TopicPartitions.map(_.topic)
    val response = getPartitionMetadata(topics).right
    val answer = response.flatMap { tms: Set[TopicMetadata] =>
      val leaderMap = tms.flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.flatMap { pm: PartitionMetadata =>
          val tp = new TopicAndPartition(tm.topic, pm.partitionId)
          if (TopicPartitions(tp)) {
            pm.leader.map { l =>
              tp -> (l.host -> l.port)
            }
          } else {
            None
          }
        }
      }.toMap

      if (leaderMap.keys.size == TopicPartitions.size) {
        Right(leaderMap)
      } else {
        val missing = TopicPartitions.diff(leaderMap.keySet)
        val err = new Err
        err.append(new SparkException(s"Couldn't find leaders for ${missing}"))
        Left(err)
      }
    }
    answer
  }

  def getPartitions(topics: Set[String]): Either[Err, Set[TopicAndPartition]] = {
    getPartitionMetadata(topics).right.map { r =>
      r.flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.map { pm: PartitionMetadata =>
          new TopicAndPartition(tm.topic, pm.partitionId)
        }
      }
    }
  }

  def getPartitionMetadata(topics: Set[String]): Either[Err, Set[TopicMetadata]] = {
    val req = TopicMetadataRequest(
      TopicMetadataRequest.CurrentVersion, 0, config.clientId, topics.toSeq)
    val errs = new Err
    withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
      val resp: TopicMetadataResponse = consumer.send(req)
      val respErrs = resp.topicsMetadata.filter(m => m.errorCode != ErrorMapping.NoError)

      if (respErrs.isEmpty) {
        return Right(resp.topicsMetadata.toSet)
      } else {
        respErrs.foreach { m =>
          val cause = ErrorMapping.exceptionFor(m.errorCode)
          val msg = s"Error getting partition metadata for '${m.topic}'. Does the topic exist?"
          errs.append(new SparkException(msg, cause))
        }
      }
    }
    Left(errs)
  }

  // Leader offset api
  // scalastyle:off
  // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI
  // scalastyle:on

  def getLatestLeaderOffsets(
                              TopicPartitions: Set[TopicAndPartition]
                              ): Either[Err, Map[TopicAndPartition, LeaderOffset]] =
    getLeaderOffsets(TopicPartitions, OffsetRequest.LatestTime)

  def getEarliestLeaderOffsets(
                                TopicPartitions: Set[TopicAndPartition]
                                ): Either[Err, Map[TopicAndPartition, LeaderOffset]] =
    getLeaderOffsets(TopicPartitions, OffsetRequest.EarliestTime)

  def getLeaderOffsets(
                        TopicPartitions: Set[TopicAndPartition],
                        before: Long
                        ): Either[Err, Map[TopicAndPartition, LeaderOffset]] = {
    getLeaderOffsets(TopicPartitions, before, 1).right.map { r =>
      r.map { kv =>
        // mapValues isnt serializable, see SI-7005
        kv._1 -> kv._2.head
      }
    }
  }

  private def flip[K, V](m: Map[K, V]): Map[V, Seq[K]] =
    m.groupBy(_._2).map { kv =>
      kv._1 -> kv._2.keys.toSeq
    }

  def getLeaderOffsets(
                        TopicPartitions: Set[TopicAndPartition],
                        before: Long,
                        maxNumOffsets: Int
                        ): Either[Err, Map[TopicAndPartition, Seq[LeaderOffset]]] = {
    findLeaders(TopicPartitions).right.flatMap { tpToLeader =>
      val leaderToTp: Map[(String, Int), Seq[TopicAndPartition]] = flip(tpToLeader)
      val leaders = leaderToTp.keys
      var result = Map[TopicAndPartition, Seq[LeaderOffset]]()
      val errs = new Err
      withBrokers(leaders, errs) { consumer =>
        val partitionsToGetOffsets: Seq[TopicAndPartition] =
          leaderToTp((consumer.host, consumer.port))
        val reqMap = partitionsToGetOffsets.map { tp: TopicAndPartition =>
          tp -> PartitionOffsetRequestInfo(before, maxNumOffsets)
        }.toMap
        val req = OffsetRequest(reqMap)
        val resp = consumer.getOffsetsBefore(req)
        val respMap = resp.partitionErrorAndOffsets
        partitionsToGetOffsets.foreach { tp: TopicAndPartition =>
          respMap.get(tp).foreach { por: PartitionOffsetsResponse =>
            if (por.error == ErrorMapping.NoError) {
              if (por.offsets.nonEmpty) {
                result += tp -> por.offsets.map { off =>
                  LeaderOffset(consumer.host, consumer.port, off)
                }
              } else {
                errs.append(new SparkException(
                  s"Empty offsets for ${tp}, is ${before} before log beginning?"))
              }
            } else {
              errs.append(ErrorMapping.exceptionFor(por.error))
            }
          }
        }
        if (result.keys.size == TopicPartitions.size) {
          return Right(result)
        }
      }
      val missing = TopicPartitions.diff(result.keySet)
      errs.append(new SparkException(s"Couldn't find leader offsets for ${missing}"))
      Left(errs)
    }
  }

  // Try a call against potentially multiple brokers, accumulating errors
  private def withBrokers(brokers: Iterable[(String, Int)], errs: Err)
                         (fn: SimpleConsumer => Any): Unit = {
    brokers.foreach { hp =>
      var consumer: SimpleConsumer = null
      try {
        consumer = connect(hp._1, hp._2)
        fn(consumer)
      } catch {
        case NonFatal(e) =>
          errs.append(e)
      } finally {
        if (consumer != null) {
          consumer.close()
        }
      }
    }
  }
}

private[kafka]
object KafkaCluster {
  type Err = ArrayBuffer[Throwable]

  /** If the result is right, return it, otherwise throw SparkException */
  def checkErrors[T](result: Either[Err, T]): T = {
    result.fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok
    )
  }

  private[kafka]
  case class LeaderOffset(host: String, port: Int, offset: Long)

  /**
   * High-level kafka consumers connect to ZK.  ConsumerConfig assumes this use case.
   * Simple consumers connect directly to brokers, but need many of the same configs.
   * This subclass won't warn about missing ZK params, or presence of broker params.
   */
  private[kafka]
  class SimpleConsumerConfig private(brokers: String, originalProps: Properties)
    extends ConsumerConfig(originalProps) {
    val seedBrokers: Array[(String, Int)] = brokers.split(",").map { hp =>
      val hpa = hp.split(":")
      if (hpa.size == 1) {
        throw new SparkException(s"Broker not the in correct format of <host>:<port> [$brokers]")
      }
      (hpa(0), hpa(1).toInt)
    }
  }

  private[kafka]
  object SimpleConsumerConfig {
    /**
     * Make a consumer config without requiring group.id or zookeeper.connect,
     * since communicating with brokers also needs common settings such as timeout
     */
    def apply(kafkaParams: Map[String, String]): SimpleConsumerConfig = {
      // These keys are from other pre-existing kafka configs for specifying brokers, accept either
      val brokers = kafkaParams.get("metadata.broker.list")
        .orElse(kafkaParams.get("bootstrap.servers"))
        .getOrElse(throw new SparkException(
        "Must specify metadata.broker.list or bootstrap.servers"))

      val props = new Properties()
      kafkaParams.foreach { case (key, value) =>
        // prevent warnings on parameters ConsumerConfig doesn't know about
        if (key != "metadata.broker.list" && key != "bootstrap.servers") {
          props.put(key, value)
        }
      }

      Seq("zookeeper.connect", "group.id").foreach { s =>
        if (!props.containsKey(s)) {
          props.setProperty(s, "")
        }
      }

      new SimpleConsumerConfig(brokers, props)
    }
  }
}

