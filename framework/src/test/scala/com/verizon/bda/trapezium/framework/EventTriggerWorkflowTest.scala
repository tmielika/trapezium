package com.verizon.bda.trapezium.framework

import java.io.{File, FileNotFoundException}

import com.verizon.bda.trapezium.framework.kafka.KafkaTestSuiteBase
import com.verizon.bda.trapezium.framework.manager.WorkflowConfig
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.io.Source

/**
  * Created by v708178 on 5/31/17.
  */
class EventTriggerWorkflowTest extends KafkaTestSuiteBase with ApplicationManagerTestSuite {
  val path1 = "src/test/data/TriggerEvents/triggerinput"
  val input1: Seq[String] = Source.fromFile(path1).mkString("").split("\n").toSeq
  def cleanOutput(path : String) : Unit = {
    val file = new File(path)
    try {
      FileUtils.forceDelete(file)
    } catch {
      case e: FileNotFoundException => logger.info("tmp file not found")
    }
  }
  test("workflowTest with batch") {
    cleanOutput("target/testdata/eventworkflow")
    cleanOutput("target/testdata/TriggeringTest")

    val workFlowToRun: WorkflowConfig = ApplicationManager.setWorkflowConfig("eventworkflow")
    createTopic("eventworkflow")
    ApplicationManager.runBatchWorkFlow(
      workFlowToRun,
      appConfig , maxIters = 1)(sc)
     val workFlowToRun1: WorkflowConfig = ApplicationManager.setWorkflowConfig("ListnerWorkFlow")
    ApplicationManager.runBatchWorkFlow(
      workFlowToRun1,
      appConfig , maxIters = 1)(sc)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.parquet("target/testdata/TriggeringTest")
    assert(df.count()>1)

  }


}
