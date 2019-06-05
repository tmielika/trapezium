package com.verizon.bda.trapezium.framework.apps

import org.apache.spark.sql.SparkSession

/**
  * Created by sankma8 on 10/11/17.
  */
object Main {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[2]").getOrCreate()


  }

}
