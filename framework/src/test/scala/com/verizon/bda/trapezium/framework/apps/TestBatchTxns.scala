/**
* Copyright (C) 2016 Verizon. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.verizon.bda.trapezium.framework.apps

import java.nio.file.{Path, Paths}
import java.sql.{Date, Time}

import com.verizon.bda.trapezium.framework.{BatchTransaction, DataSource, Trigger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory


/**
 * @author sumanth.venkatasubbaiah\
  *         venkatesh TestBatchTxn9
 *         Various Test Batch transactions
 *
 */

object TestBatchTxn1 extends BatchTransaction {

  private val CONST_STRING = "This has to be populated in the preprocess method"
  var populateFromPreprocess: String = _
  val logger = LoggerFactory.getLogger(this.getClass)
  override def preprocess(sparkSession: SparkSession): Unit = {
    logger.info("Inside preprocess of TestBatchTxn1")
    populateFromPreprocess = CONST_STRING
  }

  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("Inside process of TestBatchTxn1" + wfTime)
    require(df.size > 0)
    require(populateFromPreprocess == CONST_STRING)
    val inData = df("source1")
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    logger.info(s"Count ${df.count}")
    require(populateFromPreprocess == CONST_STRING)
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
} // end TestBatchTxn1

object TestBatchTxn2 extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("Inside process of TestBatchTxn2")
    require(df.size > 0)
    val inData = df("source2")
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    logger.info(s"Count ${df.count}")
    require(df.count == 499 )
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {

  }
} // end TestBatchTxn2


object TestBatchTxn3 extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("Inside process of TestBatchTxn3")
    require(df.size > 0)
    val inData1 = df("txn1Output")
    require(inData1.count > 0 )
    val inData2 = df("txn2Output")
    require(inData2.count > 0)
    inData1
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
} // end TestBatchTxn3

object TestBatchTxn4 extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {
    logger.info("Inside process of TestBatchTxn4" + wfTime)
    require(df.size > 0)
    val inData = df("onlyDirTrue")
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}
object TestFileSplit extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time)
  : DataFrame = {

    logger.info("Inside process of TestFileSplit" + new Date (wfTime.getTime).toString)

    require(df.size > 0)

    val inData1 = df("testDataSplitFiles")
    logger.info("count for this run is : " + inData1.count() + " worflow time is "
      + new Date (wfTime.getTime).toString)


    (inData1)
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}

object TestBatchTxn5 extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("Inside process of TestBatchTxn4" + wfTime )
    require(df.size > 0)
    val inData = df.head._2
    inData.show()
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
} // end TestBatchTxn5

object TestBatchTxn6 extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {
    logger.info("Inside process of TestBatchTxn6")
    require(df.size > 0)
    val inData = df.head._2
    inData.show()
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    df.write.parquet("/target/testdata/TestBatchTxn6")
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}

object TestBatchTxn7 extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {
    logger.info("Inside process of TestBatchTxn7")
    require(df.size > 0)
    val inData = df.head._2
    inData.show()
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    df.write.parquet("target/testdata/TestBatchTxn7")
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}

object TestBatchTxn8 extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {
    logger.info("Inside process of TestBatchTxn8")
    require(df.size > 0)
    val inData = df.head._2
    inData.show()
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    df.write.parquet("target/testdata/TestBatchTxn8")
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}

object TestBatchTxn9 extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    val currentRelativePath: Path = Paths.get("")
    val path: String = currentRelativePath.toAbsolutePath.toString
    logger.info("Current relative path is: " + path)

    logger.info("Inside process of TestBatchTxn9")
    require(df.size > 0)
    val inData = df("source1")
    inData.write.mode(SaveMode.Overwrite).save(path + "/tmp/dropRowWithExtraColumn")
    val count = inData.count()
    inData.show(false)
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}




object TestReadByDate extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {
    logger.info("Inside process of TestReadByDate  " + new Date(wfTime.getTime))
    require(df.size > 0)
    val inData = df.head._2
    logger.info("df :" + df)
    df("testDataSplitFiles").show()
    df("location").show()
    df("secondSource").show()
    inData
  }

override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
require(df.count > 0)
df.write.parquet("target/testdata/TestBatchTxn8" + System.currentTimeMillis())
  None
}

override def rollbackBatch(batchTime: Time): Unit = {
}
}



object TestBatchTxn10 extends BatchTransaction {

  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    val inData = df("source1")
    inData.show
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count == 4)
    df.write.mode(SaveMode.Overwrite).parquet("target/testdata/TestBatchTxn9/")
    None
  }

}

object TriggerTestTxn extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("InsideTriggerTestTxn")
    val inData = df("triggerTest")
    inData.show()
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    df.write.mode(SaveMode.Overwrite).parquet("target/testdata/TriggeringTest")
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}

object TestTriggering extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("Inside process of TestTriggering")
    require(df.size > 0)
    val inData = df("TestTriggering")
    inData.show()

    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    val json = new Trigger(Array(new DataSource ("triggerTest", "src/test/data/parquet")))
    logger.info("topic posted msg is " + json.toString() )
    df.write.mode(SaveMode.Overwrite).parquet("target/testdata/eventworkflow")
    Some(Array(json))
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
}
object TestParquetReadFull extends BatchTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
  override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {

    logger.info("Inside process of TestBatchTxn4" + wfTime )
    require(df.size > 0)
    val inData = df.head._2
    inData.show()
    inData
  }

  override def persistBatch(df: DataFrame, batchTime: Time): Option[Seq[Trigger]] = {
    require(df.count > 0)
    None
  }

  override def rollbackBatch(batchTime: Time): Unit = {
  }
} // end TestBatchTxn5



