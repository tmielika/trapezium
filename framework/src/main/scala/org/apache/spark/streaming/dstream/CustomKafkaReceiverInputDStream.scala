package org.apache.spark.streaming.dstream

import java.lang.Long
import java.util

import com.verizon.bda.trapezium.framework.kafka.custom.BlockMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.BlockId
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo
import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.streaming.{StreamingContext, Time}
import scala.collection.mutable.ArrayBuffer

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * NOTE: This code is adapted from ReceiverInputDStream since it was impossible to override some aspects
  *
  * A CustomReceiverInputDStream that helps to aggregate blocks based on metadata information.
  * Leverages the existing code to calculate the BlockRDD and grabs additional info to calculate
  * the OffsetRanges in the blocsks that represent the current RDD
  *
  * Created by sankma8 on 9/3/17.
  */
abstract class CustomKafkaReceiverInputDStream[T: ClassTag](_ssc: StreamingContext) extends ReceiverInputDStream[T](_ssc) {


  /**
    * The below code is copied and adapted for our Hasoffset requirement
    *
    * @param time
    * @param blockInfos
    * @return
    */
  override private[streaming] def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]): RDD[T] = {

    /**
      * FIX: Added code for computing the hasOffsetRanges
      */
    val hasOffsetRanges = computeUntilOffsets(blockInfos)

    if (blockInfos.nonEmpty) {
      val blockIds = blockInfos.map {
        _.blockId.asInstanceOf[BlockId]
      }.toArray

      // Are WAL record handles present with all the blocks
      val areWALRecordHandlesPresent = blockInfos.forall {
        _.walRecordHandleOption.nonEmpty
      }

      if (areWALRecordHandlesPresent) {
        // If all the blocks have WAL record handle, then create a WALBackedBlockRDD
        val isBlockIdValid = blockInfos.map {
          _.isBlockIdValid()
        }.toArray
        val walRecordHandles = blockInfos.map {
          _.walRecordHandleOption.get
        }.toArray
        /**
          * FIX: Adapted the type with HasOffsetRanges
          */
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, blockIds, walRecordHandles, isBlockIdValid) with HasOffsetRanges {

          override def offsetRanges: Array[OffsetRange] = hasOffsetRanges
        }
      } else {
        // Else, create a BlockRDD. However, if there are some blocks with WAL info but not
        // others then that is unexpected and log a warning accordingly.
        if (blockInfos.exists(_.walRecordHandleOption.nonEmpty)) {
          if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
            logError("Some blocks do not have Write Ahead Log information; " +
              "this is unexpected and data may not be recoverable after driver failures")
          } else {
            logWarning("Some blocks have Write Ahead Log information; this is unexpected")
          }
        }
        val validBlockIds = blockIds.filter { id =>
          ssc.sparkContext.env.blockManager.master.contains(id)
        }
        if (validBlockIds.length != blockIds.length) {
          logWarning("Some blocks could not be recovered as they were not found in memory. " +
            "To prevent such data loss, enable Write Ahead Log (see programming guide " +
            "for more details.")
        }
        /**
          * FIX: Adapted the type with HasOffsetRanges
          */
        new BlockRDD[T](ssc.sc, validBlockIds) with HasOffsetRanges {

          override def offsetRanges: Array[OffsetRange] = hasOffsetRanges
        }
      }
    } else {
      // If no block is ready now, creating WriteAheadLogBackedBlockRDD or BlockRDD
      // according to the configuration
      if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
        /**
          * FIX: Adapted the type with HasOffsetRanges
          */
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, Array.empty, Array.empty, Array.empty) with HasOffsetRanges {

          override def offsetRanges: Array[OffsetRange] = hasOffsetRanges
        }
      } else {
        /**
          * FIX: Adapted the type with HasOffsetRanges
          */
        new BlockRDD[T](ssc.sc, Array.empty) with HasOffsetRanges {

          override def offsetRanges: Array[OffsetRange] = hasOffsetRanges
        }
      }
    }
  }


  /**
    * computes the OffsetRange instances for the current RDD represents
    *
    * @param blockInfos
    * @return
    */
  private def computeUntilOffsets(blockInfos: Seq[ReceivedBlockInfo]): Array[OffsetRange] = {
    val offsetRanges = ArrayBuffer[OffsetRange]()


    var untilOffsets: util.Map[TopicPartition, Long] = new util.HashMap[TopicPartition, Long]()
    var begOffsets: util.Map[TopicPartition, Long] = new util.HashMap[TopicPartition, Long]()

    //    STEP 1 : Collect all highest untilOffsets and lowest beginning offsets
    blockInfos.foreach(block => {


      val metaDataOption = block.metadataOption
      if (metaDataOption.isDefined) {

        val blockMetadata = metaDataOption.get.asInstanceOf[BlockMetadata]

        if (blockMetadata != null) {
          //     Check and take the largest offset for untilOffset. Watermarked offsets are the
          //     last part of the block that hold the untilOffsets
          val currentOffsets = blockMetadata.getUntilOffsets()
          for ((k, v) <- currentOffsets) {
            var uOffset = v
            val previousUOffset = untilOffsets.get(k)
            if (previousUOffset != null)
              uOffset = if (v > previousUOffset) v else previousUOffset

            untilOffsets.put(k, uOffset)
          }

          //        Check and take the smallest beginning offset for a partition
          val beginningOffsets = blockMetadata.getBeginningOffsets()
          for ((k, v) <- beginningOffsets) {
            var bgOffset = v
            val previousBegOffset = begOffsets.get(k)
            if (previousBegOffset != null)
              bgOffset = if (v < previousBegOffset) v else previousBegOffset
            begOffsets.put(k, bgOffset)
          }
        }
      }

    })

    //  STEP 2 : create the offset ranges for the current set
    for ((k, v) <- untilOffsets) {
      val range = OffsetRange.create(k.topic(), k.partition(), begOffsets.get(k), untilOffsets.get(k))
      offsetRanges += range
    }

    offsetRanges.toArray
  }


}
