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
package com.verizon.bda.trapezium.framework.kafka

import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils

import scala.io.Source

/**
 * @author Pankaj on 10/29/15.
 */
class KafkaDStreamSuite extends KafkaTestSuiteBase {

  val path1 = "src/test/data/hdfs/hdfs_stream_1.csv"
  val path2 = "src/test/data/hdfs/hdfs_stream_2.csv"

  val jsonPath = "src/test/data/json/sample.json"

  val input1: Seq[String] = Source.fromFile(path1).mkString("").split("\n").toSeq
  val input2: Seq[String] = Source.fromFile(path2).mkString("").split("\n").toSeq

  val jsonInput: Seq[String] = Source.fromFile(jsonPath).mkString("").split("\n").toSeq



  test("Application Manager KAFKA Stream Test") {

    // Run primary workflow
    setupWorkflow("kafkaBatchWorkFlow", Seq(input1, input2))

    // Run dependent workflow
    setupWorkflow("kafkaStreamWorkFlow", Seq(input1, input2))

  }

  test("Application Manager KAFKA Batch Test") {

    setupWorkflow("kafkaBatchWorkFlow2", Seq(input1, input2))

  }


  test("Application Manager Kafka multiple topics test") {

    setupWorkflowForMultipleTopics("kafkaMultipleTopics",
      Seq(
        Seq(
          ("kafkaMultipleTopics_stream_1", input1),
          ("kafkaMultipleTopics_stream_2", input1)),
        Seq(
          ("kafkaMultipleTopics_stream_1", input2),
          ("kafkaMultipleTopics_stream_2", input2))
      ))
  }

  test("Application Manager Kafka multiple topics test with new topic") {

    val zkPath = s"/kafkaMultipleTopicsWithNewTopic/stream_1/0"
    ApplicationUtils.updateZookeeperValue(zkPath,
      "0",
      ApplicationManager.getConfig().zookeeperList)

    setupWorkflowForMultipleTopics("kafkaMultipleTopicsWithNewTopic",
      Seq(
        Seq(
          ("kafkaMultipleTopicsWithNewTopic_stream_1", input1),
          ("kafkaMultipleTopicsWithNewTopic_stream_2", input1)),
        Seq(
          ("kafkaMultipleTopicsWithNewTopic_stream_1", input2),
          ("kafkaMultipleTopicsWithNewTopic_stream_2", input2))
      ))
  }

  test("Read json data from Kafka") {

    setupWorkflow("readJsonFromKafka", Seq(jsonInput, jsonInput))

  }

  test("Application Manager Kafka multiple workflows with multiple topics test") {

    setupMultipleWorkflowForMultipleTopics(
        List("kafkaMultipleTopics_wf_1", "kafkaMultipleTopics_wf_2"),
      Seq(
        Seq(
          ("stream_1", input1),
          ("stream_2", input2)),
        Seq(
          ("stream_1", input1),
          ("stream_2", input2))
      ))
  }


}
