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

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig, WorkflowConfig}
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import com.verizon.bda.trapezium.validation.Validator
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.zookeeper.{KeeperException, ZooKeeper}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.{Map, Set, mutable}
import scala.collection.mutable.{Map => MMap}

import scala.collection.JavaConverters._


/**
  * @author Jiten on 10/22/15.
  *         Modified by Pankaj
  */
private[framework] object KafkaDStream {
  val logger = LoggerFactory.getLogger(this.getClass)
  var sparkcontext: Option[SparkContext] = None

  /**
    * Creates streaming context from a Kafka topic
    * Creates streaming DAG
    *
    * @return
    */
  def createStreamingContext(sparkConf: SparkConf): StreamingContext = {

    val workflowConfig = ApplicationManager.getWorkflowConfig
    sparkConf.set("spark.streaming.stopSparkContextByDefault", "false")
    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]

    if (kafkaConfig.getString("maxRatePerPartition").toInt > 0) {
      logger.info(s"maxrateperpartition - ${kafkaConfig.getString("maxRatePerPartition")}")
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition",
        kafkaConfig.getString("maxRatePerPartition"))
        .set("spark.streaming.receiver.maxRate", kafkaConfig.getString("maxRatePerPartition"))
        .set("spark.streaming.backpressure.enabled", "true")
    }
    // create new streaming context with batch duration

    var ssc: StreamingContext = null
    if (!sparkcontext.isEmpty) {

      logger.info("Using existing spark context")
      ssc = new StreamingContext(sparkcontext.get,
        Seconds(kafkaConfig.getString("batchTime").toInt))
    } else {
      logger.info("Using new spark context")
      ssc = new StreamingContext(sparkConf, Seconds(kafkaConfig.getString("batchTime").toInt))
    }
    if (sparkcontext.isEmpty) {
      sparkcontext = Some(ssc.sparkContext)
    }

    ssc

  }

  def createDStreams(ssc: StreamingContext,
                     workflowConfig: WorkflowConfig,
                     kafkabrokerlist: String,
                     kafkaConfig: Config,
                     fromOffsets: Map[TopicPartition, Long],
                     appConfig: ApplicationConfig): MMap[String, DStream[Row]] = {
    val streamsInfo = kafkaConfig.getConfigList("streamsInfo")
    logger.info(s"STREAM ${streamsInfo.toString}")

    val dStreams = scala.collection.mutable.Map[String, DStream[Row]]()

    for (off <- 0 until streamsInfo.size()) {

      val streamInfo = streamsInfo.get(off)
      val kafkaParams = buildKafkaParams(kafkabrokerlist, kafkaConfig: Config)
      val topicname = streamInfo.getString("topicName")
      val streamname = streamInfo.getString("name")
      val topicset = new scala.collection.mutable.HashSet[String]()
      topicset += topicname
      var dStreamBeginning: InputDStream[(String, String)] = null

      //      TODO: Decide if the flag here can help differentiate the type of the stream to be created

      var dStreamOffset: InputDStream[ConsumerRecord[String, String]] = {

        if ("CUSTOM".equals(workflowConfig.bridgeType))
          KafkaDStreamFactory.createCustomKafkaSparkDStream(ssc, workflowConfig, kafkaConfig,
            appConfig, kafkaParams, topicname)
        else
          KafkaDStreamFactory.createKafkaDirectStream(ssc, kafkaConfig, fromOffsets,
            appConfig, kafkaParams, topicname, getTopicOffsets)

      }

      // convert dstream of String into Row
      if (dStreamOffset != null) {
        val dStreamRow = dStreamOffset.transform((rdd) => {

          logger.info(s" Transform Count : ${rdd.count()}")
          val rowRDD = rdd.map(line => {
            logger.info(s"${line.value().toString}")
            Row(line.value().toString)
          })
          rowRDD
        })

        val topicpartitions = new scala.collection.mutable.HashMap[TopicPartition, (Long, Long)]()

        dStreamOffset.foreachRDD { rdd =>
          var rddcount = 0L;

          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          for (o <- offsetRanges) {
            topicpartitions += (new TopicPartition(o.topic, o.partition)
              -> (o.fromOffset, o.untilOffset))
            rddcount += (o.untilOffset - o.fromOffset)
          }


          appConfig.streamtopicpartionoffset += (streamname -> topicpartitions.toMap)


          logger.info(s"TOPIC PARTITIONS = ${topicpartitions.mkString(" , ")}")
          logger.info(s"Row Count ${rddcount}")
        }

        val validatedDStream = Validator.getValidatedStream(streamname, dStreamRow, streamInfo)

        dStreams += ((streamname, validatedDStream))
      } else if (dStreamBeginning != null) {

        val dStreamRow = dStreamBeginning.transform((rdd) => {

          val rowRDD = rdd.map(line => Row(line._2.toString))
          rowRDD
        })
        dStreamBeginning.foreachRDD { rdd =>
          val topicpartitions = new scala.collection.mutable.HashMap[TopicPartition, (Long, Long)]()
          var rddcount = 0L;
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          for (o <- offsetRanges) {
            topicpartitions += (new TopicPartition(o.topic, o.partition)
              -> (o.fromOffset, o.untilOffset))
            rddcount += (o.untilOffset - o.fromOffset)
          }

          appConfig.streamtopicpartionoffset += (streamname -> topicpartitions.toMap)
          logger.info(s"Row Count ${rddcount}")
        }
        val validatedDStream = Validator.getValidatedStream(streamname, dStreamRow, streamInfo)

        dStreams += ((streamname, validatedDStream))
      }
    }
    dStreams
  }

  def getTopicOffsets(fromOffsets: Map[TopicPartition, Long],
                      topicName: String): MMap[TopicPartition, Long] = {

    val topicOffsets = MMap[TopicPartition, Long]()
    val ks: Set[TopicPartition] = fromOffsets.keySet

    ks.foreach {
      x => {
        if (x.topic.equalsIgnoreCase(topicName)) {
          val off = fromOffsets.get(x)
          logger.info("Starting to read Kafka for topic - " + x.topic +
            ",  partition - " + x.partition + " from offset - " + off)

          topicOffsets.put(x, off.get)
        }
      }
    }

    topicOffsets
  }

  def fetchPartitionOffsets(kafkaTopicName: String,
                            appConfig: ApplicationConfig): Map[TopicPartition, Long] = {

    val workflowConfig = ApplicationManager.getWorkflowConfig
    val currentWorkflowKafkaPath =
      ApplicationUtils.getCurrentWorkflowKafkaPath(appConfig, kafkaTopicName, workflowConfig)

    val dependentWorkflowKafkaPath =
      ApplicationUtils.getDependentWorkflowKafkaPath(appConfig, kafkaTopicName, workflowConfig)

    fetchPartitionOffsets(kafkaTopicName, appConfig, dependentWorkflowKafkaPath, currentWorkflowKafkaPath)
  }

  def fetchPartitionOffsets(kafkaTopicName: String,
                            appConfig: ApplicationConfig,
                            dependentWorkflowKafkaPath: Option[String],
                            currentWorkflowKafkaPath: String): Map[TopicPartition, Long] = {
    var dependentTopicPartitions: mutable.HashMap[TopicPartition, Long] = null

    var currentTopicPartitions: mutable.HashMap[TopicPartition, Long] = null

    val zk = ZooKeeperConnection.create(appConfig.zookeeperList)

    try {

      ApplicationUtils.checkPath(zk, currentWorkflowKafkaPath)
      currentTopicPartitions = getPartitionsInfo(
        zk, currentWorkflowKafkaPath, kafkaTopicName, appConfig)

      if (dependentWorkflowKafkaPath != None) {

        ApplicationUtils.checkPath(zk, dependentWorkflowKafkaPath.get)
        dependentTopicPartitions =
          getPartitionsInfo(zk, dependentWorkflowKafkaPath.get, kafkaTopicName, appConfig)

        if (dependentTopicPartitions.size == 0) {
          // Batch has not executed yet - need to terminate Stream processing
          throw new Exception(
            """KafkaStream processing is being executed
              | without KafkaBatch processing""".
              stripMargin)
        }
        // Adjust the stream with respect to batch
        dependentTopicPartitions.foreach({
          case (tp
          , offset) => {
            val off =
              currentTopicPartitions.
                get(tp)
            if (off.nonEmpty) {
              currentTopicPartitions += (tp
                -> math.min(off.get, offset))
            } else {

              currentTopicPartitions += (tp -> offset)
            }
          }
        })

      }

    } catch {
      case ex@(_: KeeperException |
               _:

                 Exception) => {

        logger.error(
          "Exception", ex.getMessage)
        throw ex

      }

    }

    currentTopicPartitions.toMap
  }


  def getPartitionsInfo(zk: ZooKeeper,
                        zkNode: String,
                        kafkaTopicName: String,
                        appConfig: ApplicationConfig):
  scala.collection.mutable.HashMap[TopicPartition, Long] = {

    val topicPartitions = new scala.collection.mutable.HashMap[TopicPartition, Long]()
    val allTopicEarliest =
      getAllTopicPartitions(appConfig.kafkabrokerList, kafkaTopicName)
    val partitions = zk.getChildren(zkNode, false).asScala

    logger.info(s"Zookeeper partitions for $kafkaTopicName are ${partitions.mkString(",")}")
    for (partition <- partitions.sortWith(_.compareTo(_) < 0)) {
      val lastOffsetFromZk = zk.getData(new StringBuilder(zkNode).append("/")
        .append(partition).toString(), false, null)
      val lastOffset = new String(lastOffsetFromZk).toLong

      val currentTopicPartition = new TopicPartition(kafkaTopicName, new String(partition).toInt)
      val earliest = allTopicEarliest(currentTopicPartition)
      val offset = {
        if (earliest._2 < lastOffset) {

          logger.info(s"Earliest Kafka offset is ${earliest._2} and Zookeeper offset value " +
            s"is $lastOffset, so taking Zookeeper offset $lastOffset for streaming.")
          lastOffset
        }
        else {
          logger.warn(s"Zookeeper offset value $lastOffset is smaller than earliest Kafka " +
            s"offset ${earliest._2}, so taking Kafka offset ${earliest._2} for streaming.")
          // update zk
          ApplicationUtils.updateZookeeperValue(new StringBuilder(zkNode).append("/")
            .append(partition).toString(), earliest._2, appConfig.zookeeperList)
          earliest._2
        }
      }

      logger.info(s"Offset used for streaming for ${kafkaTopicName}.${partition} --> ${offset}")
      topicPartitions += (currentTopicPartition -> offset)
    }

    logger.info(s"Offsets used for streaming for all partitions -->" +
      s" ${topicPartitions.values.mkString(",")}")
    topicPartitions
  }

  /**
    * Saves the end kafka offsets after each batch to ZK. This method also checks
    * for new partitions added since the DStream was first created and
    * also saves the start offset of the new partition to ZK.
    * The addition of new partitions is indicated by returning "true" value,
    * otherwise a "false" value is returned
    *
    * @return "true" if new partition was added, "false" otherwise
    */
  def saveKafkaStreamOffsets(workflowConfig: WorkflowConfig): Boolean = {

    val appConfig = ApplicationManager.getConfig()

    val kafkaconfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]

    val streamsInfo = kafkaconfig.getConfigList("streamsInfo")
    logger.info(s"SaveKafkaStreamOffsets ---- ${streamsInfo}")
    var modified = false

    logger.info(s"Topic Partition read so far --> ${appConfig.streamtopicpartionoffset}")

    for (off <- 0 until streamsInfo.size()) {
      val streamInfo = streamsInfo.get(off)
      val streamname = streamInfo.getString("name")
      val topicName = streamInfo.getString("topicName")

      logger.info(s"Updating topic streamname ---- ${topicName}")
      val topicpartitions = appConfig.streamtopicpartionoffset.get(streamname)

      logger.info(s"Topic details locally ${topicpartitions.mkString("\n")}")
      if (topicpartitions.nonEmpty) {
        val newtopicpart =
          getAllTopicPartitions(appConfig.kafkabrokerList, topicName)

        logger.info(s"Topic details from broker ${newtopicpart.mkString("\n")}")

        var topicpart = topicpartitions.get

        // Add new partitions to old map
        newtopicpart.foreach { case (tp, (soff, eoff)) =>
          // New partition
          if (!topicpart.contains(tp)) {
            topicpart += (tp -> (0, eoff))
            modified = true
          }
        }

        val ks = topicpart.keySet
        var success = true
        val zkpath = ApplicationUtils.
          getCurrentWorkflowKafkaPath(appConfig, topicName, workflowConfig)

        ks.foreach { toppart =>
          val fromto = topicpart.get(toppart)
          if (fromto.nonEmpty) {
            val ft = fromto.get
            val sb = new StringBuilder(zkpath).append("/").append(toppart.partition).toString()

            val zk = ZooKeeperConnection.create(appConfig.zookeeperList)
            try {
              if (zk.exists(sb, false) == null) {

                val bs = new StringBuilder()
                val comps = sb.split("/")
                for (comp <- comps) {
                  if (comp.length() > 0) {
                    bs.append("/").append(comp)
                    if (bs.toString.equals(sb)) {
                      zk.create(sb, ft._2.toString().getBytes,
                        org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        org.apache.zookeeper.CreateMode.PERSISTENT)
                    } else {
                      if (zk.exists(bs.toString(), false) == null) {
                        zk.create(bs.toString(), "".getBytes,
                          org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE,
                          org.apache.zookeeper.CreateMode.PERSISTENT)
                      }
                    }
                  }
                }
              } else {
                zk.setData(sb, ft._2.toString().getBytes, -1)
              }
              logger.info("Saved Kafka offset for path " + sb +
                " from " + ft._1 + " to " + ft._2 + " msg count " + (ft._2.toInt - ft._1.toInt))
            } catch {
              case ex: Exception => {
                success = false
                logger.error("Failed to save Kafka offset for path " + sb +
                  " from " + ft._1 + " to " + ft._2 + " msg count " +
                  (ft._2.toInt - ft._1.toInt), ex.getMessage)
              }
            }
          }
        }
        if (success) {
          appConfig.streamtopicpartionoffset -= (streamname)
        }

      }
    }
    modified

  }

  /**
    * This method fetches earliest message offsets for all partitions of a topic
    * This purpose is to utilize the earliest offset of a newly added partition
    * and start reading from that offset in the stream. The method invoking
    * this method is supposed to figure out new partitions, if any
    * since the DStream was first created
    *
    */
  def getAllTopicPartitions(kafkabrokerlist: String, topic: String)
  : Map[TopicPartition, (Long, Long)] = {

    val kblist = MMap[String, String]()
    kblist += ("metadata.broker.list" -> getKafkaBrokerList(kafkabrokerlist))

    val kc = new KafkaCluster(kblist.toMap)
    val res = kc.getPartitions(topic.split(",").toSet)
    val topicparts = MMap[TopicPartition, (Long, Long)]()

    if (res.isRight) {
      val tp = res.right.get
      val loff = kc.getEarliestLeaderOffsets(tp)
      if (loff.isRight) {
        val off = loff.right.get
        off foreach { case (toppar, eoff) =>
          topicparts += (new TopicPartition(toppar.topic, toppar.partition) -> (0L, eoff.offset))
        }
      }
    }

    logger.info(s"Topic partition info from Broker ${topicparts}")
    topicparts.toMap

  }


  def getAllTopicPartitionsLatest(kafkabrokerlist: String, topic: String)
  : Map[TopicPartition, (Long, Long)] = {

    val kblist = MMap[String, String]()
    kblist += ("metadata.broker.list" -> getKafkaBrokerList(kafkabrokerlist))

    val kc = new KafkaCluster(kblist.toMap)
    val res = kc.getPartitions(topic.split(",").toSet)
    val topicparts = MMap[TopicPartition, (Long, Long)]()
    if (res.isRight) {
      val tp = res.right.get
      val loff = kc.getLatestLeaderOffsets(tp)
      if (loff.isRight) {
        val off = loff.right.get
        off foreach { case (toppar, eoff) => {
          logger.info("ttest" + toppar + "eoff " + eoff.offset)
        }
        }
        off foreach { case (toppar, eoff) =>
          topicparts += (new TopicPartition(toppar.topic, toppar.partition) -> (0L, (eoff.offset)))
        }
      }
    }

    logger.info(s"Topic partition info from Broker ${topicparts}")
    topicparts.toMap

  }

  private def buildKafkaParams(kafkabrokerlist: String,
                               kafkaConfig: Config): MMap[String, Object] = {
    val kafkaParams = MMap[String, Object]()

    val deserializer = "org.apache.kafka.common.serialization.StringDeserializer"

    //  Convention is as follows  key = (actual_key, calculated_value, default_value, isMandatory)
    val reservedParams = Map(
      "bootstrap.servers" -> (null, getKafkaBrokerList(kafkabrokerlist), null,true),
      "auto.offset.reset" -> (null, null, "earliest", true),
      "key.deserializer" -> (null, null, deserializer,   true),
      "value.deserializer" -> (null, null, deserializer,  true),
      "default-group" -> ("group.id",null, "consumerGroup", false)
    )

    for ((k, v) <- reservedParams) {
      var actualValue = v._2

      if(actualValue==null) {
        actualValue =
          try {
            kafkaConfig.getString(k)
          } catch {
            case ex: Throwable => {
              logger.warn(s"value for ${k} does not exist.Using the default value = '${v._3}'")
              v._3
            }
          }
      }

//      Mandatory param is empty
      if (actualValue == null && v._4)
        throw new IllegalArgumentException(s"Problems constructing the kafka params. No value defined for mandatory key '${k}'.")

      val key = {
        if(v._1!=null) v._1
        else k
      }

      kafkaParams += (key -> actualValue)
    }

    /**
      * add the rest of the kafka parameters to the params list. All the parameters
      * other than the reserved list will be added to the configuration
      */

    val consumerParams = kafkaConfig.getConfig("consumerParams")
    consumerParams.entrySet().iterator().asScala
        .filter(entry => {
          val key = entry.getKey
          !reservedParams.contains(key)
        })
      .foreach(entry => {
        val key = entry.getKey
        val paramValue = getValue(consumerParams, null, key)

        if(paramValue!=null)
          kafkaParams += (key -> paramValue)

      })

    kafkaParams
  }

  private def getValue(kafkaConfig: Config, defaultValue: String, key: String): String = {
    val key_deserializer = try {
      kafkaConfig.getString(key)
    } catch {
      case ex: Throwable => {
        logger.warn(s"${key} does not exist. Using ${defaultValue} as the default value")
        defaultValue
      }
    }
    key_deserializer
  }


  private def getKafkaBrokerList(kafkaBrokerList: String): String = {

    // for local as well as jenkins build
    // return kafka brokers with available port for local/jenkins tests
    if (ApplicationUtils.env == "local") {

      KafkaApplicationUtils.kafkaBrokers
    } else {

      kafkaBrokerList
    }
  }
}
