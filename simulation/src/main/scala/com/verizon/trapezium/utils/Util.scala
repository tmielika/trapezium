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
package com.verizon.trapezium.utils

import org.apache.spark.streaming.Duration
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Created by parmana on 4/5/16.
  */
object Util {
  val dtfPattern = "yyyy-MM-dd HH:mm:ss"
  val dtf: DateTimeFormatter = DateTimeFormat.forPattern(dtfPattern)
  val flowDuration = Duration(60 * 1000)
  def buildSliceSql(col: String, arr: Array[String]): String = {
    val bulider: StringBuilder = new StringBuilder()
    bulider.append("select ")
    for(column <- arr){
      bulider.append(column)
      bulider.append(" , ")
    }
    bulider.append("quantizeUdf(" + col + " , " + flowDuration.milliseconds + ") as quantizedtime")
    bulider.append(" from dataslice")
    bulider.toString()
  }

  def quantizeUdf(time: String, duration: Long): String = {
    val time1 = time.replace(":" , "-")
    time1.substring(0, time.length - 3)
  }

 }
