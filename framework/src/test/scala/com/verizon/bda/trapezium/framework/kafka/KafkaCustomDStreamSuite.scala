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

import scala.io.Source

/**
 * @author Pankaj on 10/29/15.
 */
class KafkaCustomDStreamSuite extends KafkaHATestSuiteBase {

  override def repeatMessageSendCount : Int  = 10

  val path1 = "src/test/data/hdfs/hdfs_stream_1.csv"
  val path2 = "src/test/data/hdfs/hdfs_stream_2.csv"

  val jsonPath = "src/test/data/json/sample.json"

  val input1: Seq[String] = Source.fromFile(path1).mkString("").split("\n").toSeq
  val input2: Seq[String] = Source.fromFile(path2).mkString("").split("\n").toSeq

  val jsonInput: Seq[String] = Source.fromFile(jsonPath).mkString("").split("\n").toSeq


  test("CustomDStream: Application Manager large input Test") {

    val inputs = Seq(input1,input2)

    setupWorkflow("kafkaPassThroughWorkFlow", inputs)
  }

  /**
    *  Defaults
    */
  test("CustomDStream: Application Manager default inputs Test") {

    val inputs = Seq(input1,input2)

    setupWorkflow("kafkaPassThroughDefaultsWorkFlow", inputs)
  }


}
