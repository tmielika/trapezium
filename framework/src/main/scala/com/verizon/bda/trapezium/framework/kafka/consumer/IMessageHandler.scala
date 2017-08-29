package com.verizon.bda.trapezium.framework.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerRecords

/**
  * Underlying implementation's Message handler to take care of working on the kafka consumer records/messages.
  *
  * @tparam K
  * @tparam V
  */

trait IMessageHandler[K,V]  {

  def handleMessage (message: ConsumerRecords[K,V])

}
