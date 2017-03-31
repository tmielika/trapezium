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
package org.apache.spark.zookeeper

import java.util.{Random, TimerTask}

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.codehaus.jettison.json.JSONObject
import org.slf4j.LoggerFactory


/**
 * Created by parmana on 1/10/17.
 */
class ChaosMonekyUtils(application_id : String ,
                       chaosMonkeyConf : Config , timer : java.util.Timer)
                      (implicit sc : SparkContext) extends TimerTask {
  val logger = LoggerFactory.getLogger(this.getClass)
  val random = new Random()
  def run() : Unit = {
    try {
    val excecutorObj = getExcecutorId
    if (excecutorObj == null) {
      return
    }
    logger.info("Killing the Excecutor for " + excecutorObj.getString("hostPort")
      + "Excecutor Id " + excecutorObj.getString("id"))
    val counter = ChaosMonekyUtils.inc
    sc.killAndReplaceExecutor(excecutorObj.getString("id"))
    if(counter == chaosMonkeyConf.getInt("maxNumberOfExcecutorToKill")) {
      logger.info("Stoping Chaos Monkey")
      timer.cancel()
    }
    logger.info("Excecutor Killed Successfully ")
    } catch {
      case ex : Exception => {
       logger.error("Error while Killing the Excecutor" , ex.getMessage)
      }
    }
  }

   def getExcecutorId() : JSONObject = {
     val exToKill = {
       var executorTokill : JSONObject = null
       try{
     logger.info("Inside the getExcecutorId ")

     val url = {
       if (chaosMonkeyConf.hasPath("sparkGenericApiUrl")) {
         chaosMonkeyConf.getString("sparkApiUrl") + "/api/v1/applications/" +
           application_id + "/executors"
       }
       else {         chaosMonkeyConf.getString("sparkApiUrl") + "/" + application_id +
           "/api/v1/applications/" + application_id + "/executors"
       }
     }
     logger.info("Spark Api Url : " +  url)

     val result = scala.io.Source.fromURL(url).mkString
     val payload = new JSONObject("{\"list\":" + result + "}").getJSONArray("list")
      var listOfExcecutors : List[JSONObject] = List()
     for(i <- 0 until payload.length){
       val obj = payload.getJSONObject(i)
       if(!obj.getString("id").equalsIgnoreCase("driver")) {
         listOfExcecutors = listOfExcecutors:+obj
       }
     }
     val index = {
       if (listOfExcecutors.size>=1) {
         random.nextInt(listOfExcecutors.size)
       } else {
         logger.info("No Excecutor are available to Kill")
         -1
       }
     }
     executorTokill = {
       if (index >= 1){
         listOfExcecutors(index)
       } else {
         null
       }
     }

       } catch {
         case ex : Exception => {
           logger.info("Error while fetching list of excecutor " +
             " for application" , ex.getMessage)
         }
       }
       executorTokill
   }
     exToKill
   }
}

object ChaosMonekyUtils {
  var counter = 0
  private def inc = { counter += 1; counter}
}
