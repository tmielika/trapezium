package com.verizon.bda.apisvcs.utils.test

import java.io.FileNotFoundException

import com.typesafe.config.ConfigFactory

import scala.io.BufferedSource

/**
  * Created by chundch on 5/22/17.
  */
object ApiServicesTestConstatns {

    val APP_CONFIGURATION = ConfigFactory.load()

    val APISVCS_TESTENDPOINT1 = APP_CONFIGURATION.getString(
    "apisvcs.akka.http.testendpoint1")

    val APISVCS_TESTENDPOINT2 = APP_CONFIGURATION.getString(
    "apisvcs.akka.http.testendpoint2")

    val APISVCS_TESTENDPOINT1_RESOURCE = APP_CONFIGURATION.getString(
    "apisvcs.akka.http.resource.testendpoint1")

    val APISVCS_TESTENDPOINT1_RESOURCE1 = APP_CONFIGURATION.getString(
    "apisvcs.akka.http.resource1.testendpoint1")

    val APISVCS_TESTENDPOINT_AUTHORIZER = APP_CONFIGURATION.getString(
    "apisvcs.akka.http.testendpoint.authorizer")

    val APISVCS_HTTP_BINDINGPORT = APP_CONFIGURATION.getInt(
    "apisvcs.akka.http.testbindingport")





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
