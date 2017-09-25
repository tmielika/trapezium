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

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.manager.WorkflowConfig
import com.verizon.trapezium.utils.Util
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
 * Created by parmana on 10/6/16.
 */
class Slice(df: DataFrame, workflowConfig: WorkflowConfig) {
  val logger = LoggerFactory.getLogger(this.getClass)
     def slice(): SaveSlice = {
     try {
       val conf = workflowConfig.workflowConfig.getConfig("dataSliceInfo")
       val sql = Util.buildSliceSql(conf.getString("slicecol"), df.schema.fieldNames)
       logger.info(sql)
       val sqlContext = df.sqlContext
       sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
       df.registerTempTable("dataslice")
       val res = sqlContext.sql(sql)
       val outputpath = conf.getString("outputpath")
       return new SaveSlice(res , workflowConfig)
     } catch {
       case ex: Exception => {
         logger.error("Error occured while slicing the data" , ex.printStackTrace())
       }
     }
     null
     }




}
