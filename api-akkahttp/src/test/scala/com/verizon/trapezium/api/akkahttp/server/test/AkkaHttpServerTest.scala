package com.verizon.trapezium.api.akkahttp.server.test

import java.util

import com.verizon.trapezium.api.akkahttp.ApiHttpServices
import com.verizon.trapezium.api.akkahttp.server.ApiAkkaHttpServer
import com.verizon.trapezium.api.akkahttp.services.test.TestEndpointService
import com.verizon.trapezium.api.akkahttp.utils.HttpServicesConstants._
import com.verizon.trapezium.api.akkahttp.utils.HttpServicesUtils._
import com.verizon.trapezium.api.akkahttp.utils.test.ApiServicesTestConstatns._
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{PostMethod, StringRequestEntity}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.verizon.bda.commons.serviceapis.security.utils.AuthorizationServicesConstants._
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext.Implicits.global
/**
  * Created by chundch on 5/18/17.
  */

@RunWith(classOf[JUnitRunner])
class AkkaHttpServerTest extends FunSuite with BeforeAndAfterAll {

  private val logger = LoggerFactory.getLogger(this.getClass)
  var apiServer: ApiAkkaHttpServer = null
  val federatedAuthorizer: String = "Facebook"
  val federatedAuthorizerToken = "J0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIs"
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
    postmethod.setRequestHeader(FEDERATED_AUTH_TYPE_HEADER_KEY,
      federatedAuthorizer)
    postmethod.setRequestHeader(FEDERATED_AUTH_TOKEN,
      federatedAuthorizerToken)

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
    postmethod.setRequestHeader(FEDERATED_AUTH_TYPE_HEADER_KEY,
      federatedAuthorizer)
    postmethod.setRequestHeader(FEDERATED_AUTH_TOKEN,
      federatedAuthorizerToken)

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
    postmethod.setRequestHeader(VZ_DATE_HEADER_KEY,
      FAILED_JWT_ASSERTION_TOKEN)
    val postdatastr = "test request test api resource of test end point two"
    val reqentity = new StringRequestEntity(postdatastr, "plain/text" , "utf-8")
    postmethod.setRequestEntity(reqentity)
    val rescode = client.executeMethod(postmethod)
    assert(200  != rescode)

  }





  override def afterAll(): Unit = {

    super.afterAll()
    apiServer.stop()

  }






}
