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
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.spark.mllib.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.TestUtils
import org.scalatest.{BeforeAndAfterAll, Suite}

trait LocalSparkSession extends BeforeAndAfterAll  { self: Suite =>
  @transient var spark: SparkSession = _

  override def beforeAll() {
    super.beforeAll()
    val conf = TestUtils.getSparkConf()
    spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
  }

  override def afterAll() {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }
}
