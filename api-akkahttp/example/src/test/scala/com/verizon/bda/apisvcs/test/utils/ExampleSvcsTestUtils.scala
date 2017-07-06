package com.verizon.bda.apisvcs.test.utils

import java.io.FileNotFoundException

import scala.io.BufferedSource

/**
  * Created by chundch on 5/26/17.
  */
object ExampleSvcsTestUtils {

  /**
    * Helper method to retrieve
    * jwt token data from file.
    * @return String
    */

  def getJwtAssertionToken(testDataPath : String , testDataFile : String) : String = {

    var testtxtsrc: BufferedSource = null
    var testdataFromFile: String = null
    try {
      testtxtsrc = scala.io.Source.fromFile(testDataPath + testDataFile)
      testdataFromFile = testtxtsrc.mkString.trim
    } catch {
      case e: FileNotFoundException => {

      }
    } finally {
      try {
        testtxtsrc.close()
      } catch {
        case e: Exception => {

        }
      }
    }
    testdataFromFile
  }



}
