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
package com.verizon.trapezium.growth

/**
  * Created by venkatesh on 10/4/16.
  */
trait DefaultUdfs extends java.io.Serializable {
  // TODO: Use SparkSQLType to clean up multiple closures
  val closureFuncForString: (String, Int) => String = (input, iter) => {
    input + iter
  }
  val closureFuncForInt: (Int, Int) => Int = (input, iter) => {
    input + iter
  }
  val closureFuncForDouble: (Double, Int) => Double = (input, iter) => {
    input + iter
  }
  val closureFuncForLong: (Long, Int) => Long = (input, iter) => {
    input + iter
  }

}
