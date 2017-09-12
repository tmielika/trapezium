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
import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig, WorkflowConfig}
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils

import scala.io.Source

/**
 * @author Pankaj on 10/29/15.
 */
class KafkaCustomDStreamSuite extends KafkaTestSuiteBase {

  val path1 = "src/test/data/hdfs/hdfs_stream_1.csv"
  val path2 = "src/test/data/hdfs/hdfs_stream_2.csv"

  val jsonPath = "src/test/data/json/sample.json"

  val input1: Seq[String] = Source.fromFile(path1).mkString("").split("\n").toSeq
  val input2: Seq[String] = Source.fromFile(path2).mkString("").split("\n").toSeq

  val jsonInput: Seq[String] = Source.fromFile(jsonPath).mkString("").split("\n").toSeq


  /**
    * adapts the workflow configuration of the existing set of tests to
    * use the new bridge type configuration
    * @param workflow
    * @param appConfig
    */
  def adapt (workflow: WorkflowConfig, appConfig: ApplicationConfig) : Unit = {
    workflow.bridgeType = "CUSTOM"
  }

  test("CustomDStream: Application Manager KAFKA Stream Test") {

    // Run primary workflow
    setupWorkflow("kafkaBatchWorkFlow", Seq(input1, input2), adapt)

    // Run dependent workflow
    setupWorkflow("kafkaStreamWorkFlow", Seq(input1, input2), adapt)

  }

  test("CustomDStream: Application Manager KAFKA Batch Test") {

    setupWorkflow("kafkaBatchWorkFlow", Seq(input1, input2), adapt)
  }

  test("CustomDStream: Application Manager Kafka multiple topics test") {

    setupWorkflowForMultipleTopics("kafkaMultipleTopics",
      Seq(
        Seq(
          ("stream_1", input1),
          ("stream_2", input1)),
        Seq(
          ("stream_1", input2),
          ("stream_2", input2))
      ) , adapt)
  }

  test("CustomDStream: Application Manager Kafka multiple topics test with new topic") {

    val zkPath = s"/kafkaMultipleTopicsWithNewTopic/stream_1/0"
    ApplicationUtils.updateZookeeperValue(zkPath,
      "0",
      ApplicationManager.getConfig().zookeeperList)

    setupWorkflowForMultipleTopics("kafkaMultipleTopicsWithNewTopic",
      Seq(
        Seq(
          ("stream_1", input1),
          ("stream_2", input1)),
        Seq(
          ("stream_1", input2),
          ("stream_2", input2))
      ), adapt)
  }

  test("CustomDStream: Read json data from Kafka") {

    setupWorkflow("readJsonFromKafka", Seq(jsonInput, jsonInput), adapt)

  }

  test("CustomDStream: Application Manager Kafka multiple workflows with multiple topics test") {

    setupMultipleWorkflowForMultipleTopics(
      List("kafkaMultipleTopics_wf_1", "kafkaMultipleTopics_wf_2"),
      Seq(
        Seq(
          ("stream_1", input1),
          ("stream_2", input2)),
        Seq(
          ("stream_1", input1),
          ("stream_2", input2))
      ), adapt)
  }

}
