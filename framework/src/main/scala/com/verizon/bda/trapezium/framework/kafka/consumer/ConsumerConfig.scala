package com.verizon.bda.trapezium.framework.kafka.consumer

import java.util
import java.util.Properties

/**
  * A configuration for creating and managing the kafka consumer.
  *
  * If maxRecordsSize is less than 1 then all records for that partition will be accumulated in to a block
  *
  *
  * Created by sankma8 on 8/7/17.
  */
class ConsumerConfig(props: Properties,
                     topics: util.Collection[String] ,
                     pollTime: Long ,
                     waitTimeBetweenPolls: Long ,
                     maxRecordSize: Long
                     ) extends Serializable {

  def getPollTime(): Long = {
    pollTime
  }
  def getConsumerProperties() : Properties =  {
    props
  }

  def getTopics () : util.Collection[String] = {
    topics
  }

  def getMaxRecordCount() : Long = {
    maxRecordSize
  }

  def getWaitTimeBetweenPolls() : Long = {
    waitTimeBetweenPolls

  }
}
