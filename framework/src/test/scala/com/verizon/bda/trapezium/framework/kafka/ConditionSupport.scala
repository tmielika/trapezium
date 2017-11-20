/**
  * Copyright (C) 2016 Verizon. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.verizon.bda.trapezium.framework.kafka
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.verizon.bda.trapezium.framework.apps.{STAGE, TestEvent, TestEventImpl}
import org.slf4j.LoggerFactory



/**
  * A condition call back support for each test. It encompasses condition checks for the tests
  */
trait ConditionSupport {

  /**
    * wait to check for any conditions
    * @param time
    */
  def await(time:Long)

  /**
    * notifies whenever an event has occurred
    * @param event
    */
  def notifyEvent(event: TestEvent)


  /**
    * Check to see if the condition checks/tasks are completed.
    * @return
    */
  def isCompleted(): Boolean

  /**
    * Test/Verify the conditions. Raise asserts/exceptions as required if verification criteria is not met
    */
  def verify()
}

/**
  * a delegating conditions set tool
  * @param conditions
  */
class ComplexConditionSupportSet(conditions : List[ConditionSupport]) extends ConditionSupport {

  override def await(time: Long): Unit = {
    conditions.foreach(_.await(time))
  }

  override def notifyEvent(event: TestEvent): Unit = {
    conditions.foreach(_.notifyEvent(event))
  }

  override def isCompleted(): Boolean = {
    var result = true
    conditions.foreach( result &=  _.isCompleted())
    result
  }

  override def verify(): Unit = {
    conditions.foreach(_.verify())
  }
}

/**
  * Checks for events that are coming only from the persistStream() method callback
  * @param messages
  */
class PersistStreamCheckConditionSupport(messages:Long) extends ConditionSupport {
  var total_messages = 0L
  val logger = LoggerFactory.getLogger(this.getClass)

  val latch = new CountDownLatch(messages.toInt)

  override def await(wait:Long) = latch.await(wait, TimeUnit.SECONDS)

  override def notifyEvent(condition: TestEvent): Unit =  {

    if(! condition.isInstanceOf[TestEventImpl])
      return

    val event: TestEventImpl = condition.asInstanceOf[TestEventImpl]

    /**
      * look only for persistStream callbacks
      */
    if(event.stage != STAGE.persistStream)
      return

    var count = event.count
    total_messages += count
    logger.info(s" [${event.name}] ; current_latch = ${latch.getCount} ; Batch = ${event.batch} ; message_count = ${event.count} ;  expected = ${messages} ; totalTillNow = ${total_messages}")
    while (count > 0) {
      latch.countDown()
      count -= 1
    }
  }

  override def isCompleted(): Boolean = (latch.getCount == 0)

  override def verify(): Unit = {
        assert(latch.getCount == 0, s"received only ${messages - latch.getCount} out of ${messages}")
  }
}

