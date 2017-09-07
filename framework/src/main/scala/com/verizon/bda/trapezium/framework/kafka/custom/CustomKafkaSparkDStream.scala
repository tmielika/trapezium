package com.verizon.bda.trapezium.framework.kafka.custom

import com.verizon.bda.trapezium.framework.kafka.consumer.{BalancedKafkaConsumer, ConsumerConfig}
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{CustomKafkaReceiverInputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{StreamingContext, Time}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by sankma8 on 7/28/17.
  */
object CustomKafkaSparkDStream {

  def createDStream[K: ClassTag, V: ClassTag](ssc: StreamingContext,
                                              consumerConfig: ConsumerConfig,
                                              appConfig: ApplicationConfig,
                                              workflowName: String,
                                              syncWorkflow: String): ReceiverInputDStream[ConsumerRecord[K, V]] = {


    new CustomInputStream[K, V](ssc, consumerConfig,  appConfig, workflowName, syncWorkflow)
  }

  // --------- START : Block manager based IMPL ---------

  /**
    * A customInputDStream. To be enhanced based on the need such as rate controllers, block creations, RDD etc.
    * @param ssc
    * @param consumerConfig
    * @param appConfig
    * @tparam K
    * @tparam V
    */
  class CustomInputStream[K: ClassTag, V: ClassTag](ssc: StreamingContext,
                                                    consumerConfig: ConsumerConfig,
                                                    appConfig: ApplicationConfig,
                                                    workflowName: String,
                                                    syncWorkflow: String)
    extends CustomKafkaReceiverInputDStream[ConsumerRecord[K, V]](ssc) {

    override def getReceiver(): Receiver[ConsumerRecord[K, V]] =
      new MyKafkaConsumer[K, V](ssc, consumerConfig,  appConfig,workflowName, syncWorkflow)

    /**
      * overriding to catch the RDD information
      * @param validTime
      * @return
      */
    override def compute(validTime: Time): Option[RDD[ConsumerRecord[K, V]]] = {

      val rdd = super.compute(validTime)
      rdd
    }

  }

  class MyKafkaConsumer[K: ClassTag, V: ClassTag](ssc: StreamingContext,
                                                  consumerConfig: ConsumerConfig,
                                                  appConfig: ApplicationConfig,
                                                  workflowName: String,
                                                  syncWorkflow: String)
    extends Receiver[ConsumerRecord[K, V]](StorageLevel.MEMORY_AND_DISK) {

    lazy val logger = LoggerFactory.getLogger(this.getClass)

    var balancedKafkaConsumer:BalancedKafkaConsumer[K, V] = _



    /**
      * stores in to the Spark block manager as a consumer record to keep consistent
      * with the KafkaUtil.createDStream() API.
      * At the same time keep the ArrayBuffer to represent the block of messages for
      * reliability as per spark
      */
    def storeRecord(record: ArrayBuffer[ConsumerRecord[K, V]],
                    blockMetadata : BlockMetadata) : Unit = {

      //TODO: Change to Debug before checkin
      logger.info(s"storing [${record.size}]  messages")
      store(record, blockMetadata)
    }

    override def onStart(): Unit = {
      logger.info(s"Starting the reciver @ ${this.streamId}")
      val blockWriter:IBlockWriter[K,V]= BlockWriterFactory.createDefaultBlockWriter[K,V](consumerConfig, storeRecord)
      balancedKafkaConsumer = new BalancedKafkaConsumer[K, V](
        consumerConfig,
        new ZkBasedOffsetManager( appConfig, workflowName, syncWorkflow),
        new MessageHandler[K, V](blockWriter))
      balancedKafkaConsumer.start()
    }

    override def onStop(): Unit = {
      balancedKafkaConsumer.shutdown();
    }
  }
}
