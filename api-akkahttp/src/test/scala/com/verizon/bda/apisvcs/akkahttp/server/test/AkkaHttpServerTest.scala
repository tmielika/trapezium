package com.verizon.bda.apisvcs.akkahttp.server.test

import com.verizon.bda.apisvcs.ApiHttpServices
import com.verizon.bda.apisvcs.akkahttp.server.ApiAkkaHttpServer
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import java.util

import akka.http.scaladsl.model.StatusCodes
import com.verizon.bda.apisvcs.utils.test.ApiServicesTestConstatns._
import com.verizon.bda.apisvcs.akkahttp.serveices.test.TestEndpointService
import com.verizon.bda.apisvcs.utils.HttpServicesConstants._
import com.verizon.bda.apisvcs.utils.HttpServicesUtils._
import com.verizon.bda.wso2impl.util.JWTParseHelper
import com.verizon.logger.BDALoggerFactory
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{FileRequestEntity, GetMethod, PostMethod, StringRequestEntity}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by chundch on 5/18/17.
  */

@RunWith(classOf[JUnitRunner])
class AkkaHttpServerTest extends FunSuite with BeforeAndAfterAll {

  private val logger = BDALoggerFactory.getLogger(this.getClass)
  var apiServer: ApiAkkaHttpServer = null
  val JWTTOKEN_DATA_FILE_PATH = "src/test/data/"
  val JWTTOKEN_DATA_FILE = "wso2_assertiontoken.txt"
  var JWT_ASSERTION_TOKEN : String = null
  var FAILED_JWT_ASSERTION_TOKEN : String = null
  val hostname: String = getHostName
  var svcsToPublish : util.HashMap[String, ApiHttpServices] =
    new util.HashMap[String, ApiHttpServices]()

  /**
    * Helper to initialize required
    * object for the test
    */

  override def beforeAll() {
    super.beforeAll()
    svcsToPublish.put(APISVCS_TESTENDPOINT1, new TestEndpointService)
    svcsToPublish.put(APISVCS_TESTENDPOINT2, new TestEndpointService)
    apiServer = new ApiAkkaHttpServer
    apiServer.init(svcsToPublish)
    JWT_ASSERTION_TOKEN = getJwtAssertionToken(JWTTOKEN_DATA_FILE_PATH, JWTTOKEN_DATA_FILE)
    FAILED_JWT_ASSERTION_TOKEN =
      "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImFfamhOdXMyMUtWdW9GeDY1TG1rVzJPX2wxMCJ9"
    apiServer.start(getHostName, APISVCS_HTTP_BINDINGPORT)
  }

   /**
    * Test http service for tes endpoint 1
    */

  test("validate http services for test endpoint : " + APISVCS_TESTENDPOINT1) {

    logger.info("Testing http service for test endpoint 1 :" + APISVCS_TESTENDPOINT1)
     val noofroutes = 2
    val postendpoint = "http://" + hostname + ":" +
      APISVCS_HTTP_BINDINGPORT + "/" + APISVCS_TESTENDPOINT1 + "/" +
      APISVCS_TESTENDPOINT1_RESOURCE
    val client = new HttpClient
    val postmethod = new PostMethod(postendpoint)
    postmethod.setRequestHeader(WSO2_AUTHORIZATION_DATA_KEY,
      JWT_ASSERTION_TOKEN)
    val postdatastr = "test request test api resource of test end point one"
    val reqentity = new StringRequestEntity(postdatastr, "plain/text" , "utf-8")
    postmethod.setRequestEntity(reqentity)
    val rescode = client.executeMethod(postmethod)
    assert(200 == rescode)
    val resdata = postmethod.getResponseBodyAsString
    assert(resdata.contains(postdatastr))

  }


  /**
    * Test http service for tes endpoint 2
    */

  test("validate http services for test endpoint : " + APISVCS_TESTENDPOINT2) {

    logger.info("Testing http service for test endpoint 1 :" + APISVCS_TESTENDPOINT2)
    val noofroutes = 2
    val postendpoint = "http://" + hostname + ":" +
      APISVCS_HTTP_BINDINGPORT + "/" + APISVCS_TESTENDPOINT2 + "/" +
      APISVCS_TESTENDPOINT1_RESOURCE
    val client = new HttpClient
    val postmethod = new PostMethod(postendpoint)
    postmethod.setRequestHeader(WSO2_AUTHORIZATION_DATA_KEY,
      JWT_ASSERTION_TOKEN)
    val postdatastr = "test request test api resource of test end point two"
    val reqentity = new StringRequestEntity(postdatastr, "plain/text" , "utf-8")
    postmethod.setRequestEntity(reqentity)
    val rescode = client.executeMethod(postmethod)
    assert(200  == rescode)
    val resdata = postmethod.getResponseBodyAsString
    assert(resdata.contains(postdatastr))

  }

  /**
    * Test http service for tes endpoint 2
    */

  test("validate failed authentication failed test endpoint : " + APISVCS_TESTENDPOINT1) {

    logger.info("Testing http service for test endpoint 1 :" + APISVCS_TESTENDPOINT1)
    val noofroutes = 2
    val postendpoint = "http://" + hostname + ":" +
      APISVCS_HTTP_BINDINGPORT + "/" + APISVCS_TESTENDPOINT1 + "/" +
      APISVCS_TESTENDPOINT1_RESOURCE
    val client = new HttpClient
    val postmethod = new PostMethod(postendpoint)
    postmethod.setRequestHeader(WSO2_AUTHORIZATION_DATA_KEY,
      FAILED_JWT_ASSERTION_TOKEN)
    val postdatastr = "test request test api resource of test end point two"
    val reqentity = new StringRequestEntity(postdatastr, "plain/text" , "utf-8")
    postmethod.setRequestEntity(reqentity)
    val rescode = client.executeMethod(postmethod)
    assert(200  != rescode)

  }


  test("jwt token parse testing") {
    logger.info("Testing jwt token parsing ")
    try {
      val helper = new JWTParseHelper();
      val jwtdata = helper.parseJWTData(JWT_ASSERTION_TOKEN);
      assert(null != jwtdata)
      assert(jwtdata.size() == 13)

    } catch {
      case e: Exception => {
        logger.error("parse test failed", e)
        fail()
      }

    }

  }





  override def afterAll(): Unit = {

    super.afterAll()
    apiServer.stop()

  }






}
