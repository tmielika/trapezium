package com.verizon.bda.trapezium.framework.apps

import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Created by sankma8 on 11/13/17.
  */
object TestConditionManager {
  val logger = LoggerFactory.getLogger(this.getClass)

  val listeners = new ListBuffer[ITestEventListener]()

  def addListener(listener: ITestEventListener): Unit = {
    logger.info("Adding listener .....")
    listeners += (listener)
  }

  def removeListener(listener: ITestEventListener): Unit = {
    logger.info("removing listener .....")
    listeners -= (listener)
  }

  def notify(event: TestEvent): Unit = {
    logger.info("notifying listener .....")
    listeners.foreach(_.notify(event))
  }
}

trait ITestEventListener {
  def getName(): String
  def notify(event: TestEvent)
}

trait TestEvent {
  def getEventType(): String
}

/**
  * Stage represents a stage in the callback for the transaction
  */
object STAGE extends Enumeration {
  type Stage = Value
  val preprocess, processStream, persistStream, rollbackStream, OTHER = Value
}

/**
  * a more generic event with an extra map
  *
  * @param map
  */
class TestEventImpl(val name: String, val stage: STAGE.Value, val batch: Int, val count: Long, val map: Map[String, String]) extends TestEvent {


  def getMap(): Map[String, String] = {
    map
  }

  override def getEventType(): String = "vz.KafkaTestEvent"
}


object TestEventFactory {

  def createTestEventMap(name: String, stage: STAGE.Value, batch: Int, count: Long): TestEvent = {
    val countVal = count.toInt.toString
    val map = Map[String,String]()
    new TestEventImpl(name, stage, batch,count,map)
  }

}

