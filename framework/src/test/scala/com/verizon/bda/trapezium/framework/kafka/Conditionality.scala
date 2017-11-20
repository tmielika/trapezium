package com.verizon.bda.trapezium.framework.kafka

import com.verizon.bda.trapezium.framework.apps.{ITestEventListener, TestEvent}

/**
 * Created by sankma8 on 11/17/17.
 */

case class Conditionality ( condition:ConditionSupport, listener:ITestEventListener)

object ConditionalityFactory {

  def createDefaultTestCondition(size: Int): Conditionality = {

    val condition = new PersistStreamCheckConditionSupport(size.toLong)

    val listener = new ITestEventListener {

      override def notify(event: TestEvent): Unit = {

        condition.notifyEvent(event)
      }

      override def getName(): String = "generic.test.listener"
    }

    Conditionality( condition, listener)
  }

  def createEmptyTestCondition(): Conditionality = {
    Conditionality( null, null)
  }

}
