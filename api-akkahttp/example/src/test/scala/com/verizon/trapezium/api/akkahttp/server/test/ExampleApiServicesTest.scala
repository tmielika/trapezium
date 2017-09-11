package com.verizon.trapezium.api.akkahttp.server.test

import java.util

import com.verizon.trapezium.api.akkahttp.ApiHttpServices
import com.verizon.trapezium.api.akkahttp.utils.HttpServicesUtils._
import com.verizon.trapezium.api.SampleRouteHttpServices
import com.verizon.trapezium.api.akkahttp.server.ApiAkkaHttpServer
import com.verizon.trapezium.api.akkahttp.utils.HttpServicesConstants.{WSO2_AUTHORIZATION_DATA_KEY => _}
import com.verizon.trapezium.api.test.utils.ExampleSvcsTestUtils._
import com.verizon.trapezium.api.utils.SampleHttpSvcsConstans._
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{PostMethod, StringRequestEntity}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Created by chundch on 4/27/17.
  */


@RunWith(classOf[JUnitRunner])
class ExampleApiServicesTest extends FunSuite with BeforeAndAfterAll {

  private val logger = LoggerFactory.getLogger(this.getClass)

  var apiServer: ApiAkkaHttpServer = null
  val JWTTOKEN_DATA_FILE_PATH = "src/test/data/"
  val JWTTOKEN_DATA_FILE = "wso2_assertiontoken.txt"
  val SAMPLE_SVC_HEADER_DATE_DATA = "2017-08-28 15:40"
  val SAMPLE_SVC_AUTHZ_DATA = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImF"
  var JWT_ASSERTION_TOKEN : String = null
  val hostname: String = "10.20.210.68" // getHostName
  var svcsToPublish : util.HashMap[String, ApiHttpServices] =
  new util.HashMap[String, ApiHttpServices]()

  /**
    * Helper to initialize required
    * object for the test
    */

  override def beforeAll() {
    super.beforeAll()
    svcsToPublish.put(SAMPLE_HTTP_SERVICE_PUBLISH_ENDPOINT, new SampleRouteHttpServices)
    apiServer = new ApiAkkaHttpServer
    apiServer.init(svcsToPublish)
    JWT_ASSERTION_TOKEN = getJwtAssertionToken(JWTTOKEN_DATA_FILE_PATH, JWTTOKEN_DATA_FILE)
    apiServer.start(getHostName, SAMPLE_HTTP_SERVICE_BINDING_PORT)
  }

  /**
    * Test example http service for tes endpoint
    */

  test("validate http services for test endpoint : " + SAMPLE_HTTP_SERVICE_PUBLISH_ENDPOINT) {

    logger.info("Testing example http service endpoint :" + SAMPLE_HTTP_SERVICE_PUBLISH_ENDPOINT)
    val noofroutes = 2
    val postendpoint = "http://" + hostname + ":" +
      SAMPLE_HTTP_SERVICE_BINDING_PORT + "/" + SAMPLE_HTTP_SERVICE_PUBLISH_ENDPOINT + "/" +
      SAMPLE_HTTP_SERVICE_RESOURCE
    val client = new HttpClient
    val postmethod = new PostMethod(postendpoint)
    postmethod.setRequestHeader(SAMPLE_SVCS_DATE_HEADER_KEY,
      SAMPLE_SVC_HEADER_DATE_DATA)
    postmethod.setRequestHeader(SAMPLE_SVCS_AUTHORIZATION_HEADER_KEY,
      SAMPLE_SVC_AUTHZ_DATA)
    val postdatastr = "test request test example service sample resource "
    val reqentity = new StringRequestEntity(postdatastr, "plain/text" , "utf-8")
    postmethod.setRequestEntity(reqentity)
    val rescode = client.executeMethod(postmethod)
    assert(200 == rescode)
    val resdata = postmethod.getResponseBodyAsString
    assert(resdata.contains(postdatastr))

  }

  override def afterAll(): Unit = {

    super.afterAll()
    apiServer.stop()

  }




}
