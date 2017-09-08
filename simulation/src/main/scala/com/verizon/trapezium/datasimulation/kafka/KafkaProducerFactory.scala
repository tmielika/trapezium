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
package com.verizon.trapezium.datasimulation.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Created by parmana on 10/12/16.
 */
object KafkaProducerFactory {

  // scalastyle:off
  import scala.collection.JavaConversions._
  // scalastyle:on
  val logger = LoggerFactory.getLogger(this.getClass)

  private val producers = mutable.Map[Map[String, String], KafkaProducer[String, String]]()

  def getOrCreateProducer(config: Map[String, String]): KafkaProducer[String, String] = {

    val defaultConfig = Map(
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )

    val finalConfig = defaultConfig ++ config
    producers.getOrElseUpdate(finalConfig, {
      logger.info(s"Create Kafka producer , config: $finalConfig")
      val producer = new KafkaProducer[String, String](finalConfig)

      sys.addShutdownHook {
        logger.info(s"Close Kafka producer, config: $finalConfig")
        producer.close()
      }

      producer
    })
  }
}
