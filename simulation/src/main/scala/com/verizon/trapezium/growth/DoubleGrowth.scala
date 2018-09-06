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

import org.apache.spark.sql
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by venkatesh on 10/6/16.
  */
trait DoubleGrowth extends Growth[Double] with Serializable {

  import org.apache.spark.sql.functions._

   def customUDF: UserDefinedFunction = {
    udf((inputData: Double, rand: Int) => customFunction(inputData, rand))
  }
}
