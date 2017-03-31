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
package com.verizon.bda.datasimulation.kafka
import org.apache.kafka.clients.producer.{RecordMetadata, Callback, ProducerRecord}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD


/**
 * Created by parmana on 10/12/16.
 */
class SaveRdd (rdd: RDD[String]) extends Serializable {

  def sendToKafka(config: Map[String, String], topic: String): Unit = {

    rdd.foreachPartition { records =>
      val producer = KafkaProducerFactory.getOrCreateProducer(config)
      val logger = Logger.getLogger(getClass)

      val callback = new KafkaDStreamSinkExceptionHandler
      val metadata = records.map { record =>
        callback.throwExceptionIfAny()
        producer.send(new ProducerRecord(topic, "key", record), callback)
      }.toList

      metadata.foreach { metadata => metadata.get() }

      callback.throwExceptionIfAny()
    }
  }
}
class KafkaDStreamSinkExceptionHandler extends Callback {

  import java.util.concurrent.atomic.AtomicReference

  private val lastException = new AtomicReference[Option[Exception]](None)

  override def onCompletion(metadata: RecordMetadata, exception: Exception):
  Unit = lastException.set(Option(exception))

  def throwExceptionIfAny(): Unit = lastException.getAndSet(None).foreach(ex => throw ex)
}
