package com.verizon.bda.trapezium.framework.apps

import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Created by sankma8 on 11/13/17.
  */
object TestConditionManager {
  val logger = LoggerFactory.getLogger(this.getClass)

  val listeners = new ListBuffer[ITestListener]()

  def addListener (listener: ITestListener): Unit = {
    logger.info("Adding listener .....")
    listeners +=(listener)
  }

  def removeListener (listener: ITestListener): Unit = {
    logger.info("removing listener .....")
    listeners -=(listener)
  }

  def notify(condition: ITestCondition): Unit = {
    logger.info("notifying listener .....")
    listeners.foreach( _.notify(condition))
  }

}

trait ITestListener {
  def notify(condition: ITestCondition)
}

trait ITestCondition {

}

class TestConditionMap(val map:Map[String,String]) extends ITestCondition {

  def getMap(): Map[String,String] = {
    map
  }

}

object TestConditionFactory {

  object KEYS extends Enumeration {
    type keys = Value
    val CLASS_NAME, STAGE, BATCH, COUNT, OTHER = Value
  }

  def createCondition (name:String, stage:String, batch:Int, count:Long) : ITestCondition = {
    val countVal = count.toInt.toString
    val map = Map(KEYS.CLASS_NAME.toString -> name , KEYS.STAGE.toString -> stage, KEYS.BATCH.toString -> batch.toString, KEYS.COUNT.toString -> countVal)
    new TestConditionMap(map)
  }

}

