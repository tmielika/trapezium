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
package com.verizon.bda.trapezium.framework

import java.util.Calendar

import com.verizon.bda.trapezium.framework.handler.{BatchHandler, FileCopy}
import com.verizon.bda.trapezium.framework.manager.WorkflowConfig
import org.apache.spark.sql.SQLContext
import java.util.{Calendar, Date, Timer, TimerTask}
import org.slf4j.LoggerFactory

import scala.collection.mutable.{Map => MMap}

/**
  * @author hutashan DelaySuite
  */
class DelaySuite extends ApplicationManagerTestSuite  {


  var startTime = System.currentTimeMillis()-500000
  override def beforeAll(): Unit = {
    super.beforeAll()

  }

  test("Delay Test current time ") {
    val logger = LoggerFactory.getLogger(this.getClass)
    val calendar = Calendar.getInstance()
    val curHour = calendar.get(Calendar.HOUR_OF_DAY)
    val newCur = curHour + 1
    logger.info("curHr1 " + newCur)
    val delayTime = s"$newCur:20".split(":")
    val timeDelay = BatchHandler.getdtNextRun(delayTime)
    logger.info("TimeDelay is  " + timeDelay )
    calendar.set(Calendar.HOUR_OF_DAY, newCur)
    calendar.set(Calendar.MINUTE, 20)
    calendar.set(Calendar.SECOND, 0)
    logger.info("Cal + " + calendar.getTime())
    assert(calendar.getTime().getTime/1000==timeDelay.getTime/1000)

  }

  test("delay hour") {
    val calendar = Calendar.getInstance()
    val curHr = (calendar.get(Calendar.HOUR_OF_DAY))
    val newC = curHr-1
    logger.info("curHr " + newC)
    val dtSchedule = s"$newC:20".split(":")
    val timeDl = BatchHandler.getdtNextRun(dtSchedule)
    logger.info("TimeDelay is second  " + timeDl )
    calendar.set(Calendar.HOUR_OF_DAY , newC + 24)
    calendar.set(Calendar.MINUTE, 20)
    calendar.set(Calendar.SECOND, 0)
    logger.info("Cal + " + calendar.getTime())
    assert(calendar.getTime().getTime/1000==timeDl.getTime/1000)
    assert(calendar.getTime().getTime/1000==timeDl.getTime/1000)
    val cal = Calendar.getInstance()
    assert(timeDl.getTime>cal.getTime().getTime)
  }


  test("delay min") {
    val calendar = Calendar.getInstance()
    val curHr = (calendar.get(Calendar.HOUR_OF_DAY))
    val curMin = (calendar.get(Calendar.MINUTE))
    val newC = curMin-10
    logger.info("curHr " + newC)
    val dtSchedule = s"$curHr:$newC".split(":")
    val timeDl = BatchHandler.getdtNextRun(dtSchedule)
    logger.info("TimeDelay is second  " + timeDl )
    logger.info("current time is second  " + timeDl )
    calendar.set(Calendar.HOUR_OF_DAY , curHr + 24)
    calendar.set(Calendar.MINUTE, newC)
    calendar.set(Calendar.SECOND, 0)
    logger.info("Cal + " + calendar.getTime())
    assert(calendar.getTime().getTime/1000==timeDl.getTime/1000)
    assert(calendar.getTime().getTime/1000==timeDl.getTime/1000)
    val cal = Calendar.getInstance()
    assert(timeDl.getTime>cal.getTime().getTime)
  }



  override def afterAll(): Unit = {
    super.afterAll()

  }

}

