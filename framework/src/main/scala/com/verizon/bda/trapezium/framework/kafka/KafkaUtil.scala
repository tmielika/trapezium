package com.verizon.bda.trapezium.framework.kafka

import com.verizon.bda.trapezium.framework.manager.{WorkflowConfig, ApplicationConfig}
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters.asScalaBufferConverter
import org.apache.spark.streaming.kafka.{OffsetRange}
import org.apache.zookeeper.{ZooKeeper}
import kafka.common.TopicAndPartition
/**
  * Created by v708178 on 6/6/17.
  */
object KafkaUtil {
  val logger = LoggerFactory.getLogger(this.getClass)
  def getOffsetsRange( zk: ZooKeeper,
                       zkNode: String,
                       kafkaTopicName: String,
                       appConfig: ApplicationConfig):
  List[OffsetRange] = {

    var offsetRangeList: List[OffsetRange] = Nil
    val topicPartitions = new collection.mutable.HashMap[TopicAndPartition, Long]()
    val allTopicEarliest =
      KafkaDStream.getAllTopicPartitions(appConfig.kafkabrokerList, kafkaTopicName)
    val allLatest = KafkaDStream.getAllTopicPartitionsLatest(appConfig.kafkabrokerList,
      kafkaTopicName)
    val partitions =
      try {
       zk.getChildren(zkNode, false).asScala
    } catch {
      case ex: Exception => {
        List("0")
      }
    }
    logger.info(s"Zookeeper partitions for $kafkaTopicName are ${partitions.mkString(",")}")
    for (partition <- partitions.sortWith(_.compareTo(_) < 0)) {
      val lastOffset =
        try {
          new String(zk.getData(new StringBuilder(zkNode).append("/")
            .append(partition).toString(), false, null)).toLong
        } catch {
          case ex: Exception => {
       0L
      }
    }
      val currentTopicPartition = new TopicAndPartition(kafkaTopicName, new String(partition).toInt)
      val earliest = allTopicEarliest(currentTopicPartition)
      val latest = allLatest(currentTopicPartition)
      val offset = {
        if (earliest._2 < lastOffset){
          logger.info(s"Earliest Kafka offset is ${earliest._2} and Zookeeper offset value " +
            s"is $lastOffset, so taking Zookeeper offset $lastOffset for streaming.")
          lastOffset
        }
        else {
          logger.info(s"Zookeeper offset value $lastOffset is smaller than earliest Kafka " +
            s"offset ${earliest._2}, so taking Kafka offset ${earliest._2} for streaming.")
          // update zk
          ApplicationUtils.updateZookeeperValue(new StringBuilder(zkNode).append("/")
            .append(partition).toString(), earliest._2, appConfig.zookeeperList)
          earliest._2
        }
      }

      logger.info(s"Offset used for streaming " +
        s"for ${kafkaTopicName}.${partition} --> ${offset} to ${latest._2}")
      val offsetRange: OffsetRange = OffsetRange.create(currentTopicPartition, offset, latest._2)
      offsetRangeList = offsetRangeList :+ offsetRange
    }

    logger.info(s"Offsets used for streaming for all partitions -->" +
      s" ${topicPartitions.values.mkString(",")}")
    offsetRangeList
  }


}
