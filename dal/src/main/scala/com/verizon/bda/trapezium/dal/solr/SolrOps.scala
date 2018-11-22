package com.verizon.bda.trapezium.dal.solr

import java.io.File
import java.nio.file.{Path, Paths}
import java.sql.Time
import java.util.UUID
import javax.net.ssl.SSLContext

import com.verizon.bda.trapezium.dal.exceptions.SolrOpsException
import com.verizon.bda.trapezium.dal.util.zookeeper.ZooKeeperClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.mutable.ParArray
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by venkatesh on 8/3/17.
  */
abstract class SolrOps(solrMap: Map[String, String]) {
  val solrDeployerZnode = "/solrDeployer"
  lazy val log = Logger.getLogger(classOf[SolrOps])
  val appName = solrMap("appName")
  val httpTypeSolr = solrMap("httpTypeSolr")
  var aliasCollectionName: String = null
  var collectionName: String = null
  lazy val configName = s"$appName/${aliasCollectionName}"
  var hdfsIndexFilePath: String = null
  var coreMap: Map[String, String] = null
  val lb: ListBuffer[Future[Boolean]] = new ListBuffer[Future[Boolean]]


  def upload(): Unit = {
    val solrClient = SolrClusterStatus.cloudClient
    val path: Path = Paths.get(solrMap("solrConfig"))
    log.info(s"uploading to ${configName} from path:${path.toString}")
    solrClient.uploadConfig(path, configName)
    log.info("uploaded the config successfully ")
  }

  def getSolrCollectionUrl(): String = {
    val host = SolrClusterStatus.solrLiveNodes.head
    val solrServerUrl = s"$httpTypeSolr$host/solr/admin/collections"
    solrServerUrl
  }

  def aliasCollection(): Unit = {
    val solrServerUrl = getSolrCollectionUrl()
    val aliaseCollectionUrl = s"$solrServerUrl?action=CREATEALIAS&" +
      s"name=$aliasCollectionName" + s"&collections=$collectionName"
    log.info(s"aliasing collection  ${collectionName} with ${aliasCollectionName}")
    val response = SolrOps.makeHttpRequest(aliaseCollectionUrl)
    //    storeFiledsinZk()
    ZooKeeperClient(solrMap("zkHosts"))
    if (solrMap.contains("storageDir")) {
      ZooKeeperClient.setData(s"$solrDeployerZnode/$aliasCollectionName/indicesPath",
        (solrMap("storageDir").stripSuffix("/") + "/" + collectionName).getBytes())
    }
    ZooKeeperClient.setData(s"$solrDeployerZnode/$aliasCollectionName/hdfsPath",
      hdfsIndexFilePath.getBytes())
    ZooKeeperClient.setData(s"$solrDeployerZnode/$aliasCollectionName/failureCoreCount",
      "0".getBytes())
    ZooKeeperClient.setData(s"$solrDeployerZnode/" +
      s"$aliasCollectionName/collectionFailurecount",
      "0".getBytes())
    ZooKeeperClient.setData(s"$solrDeployerZnode/" +
      s"$aliasCollectionName/assignedLiveNodes",
      s"${SolrClusterStatus.solrLiveNodes.size}".getBytes())
    require(ZooKeeperClient.getData(s"$solrDeployerZnode/" +
      s"$aliasCollectionName/failureCoreCount")
      == "0")
    require(ZooKeeperClient.getData(s"$solrDeployerZnode/" +
      s"$aliasCollectionName/hdfsPath")
      == hdfsIndexFilePath)
    require(ZooKeeperClient.getData(s"$solrDeployerZnode/" +
      s"$aliasCollectionName/assignedLiveNodes")
      == s"${SolrClusterStatus.solrLiveNodes.size}")
    ZooKeeperClient.close()
    log.info(s"aliasing collection response ${response}")
  }

  def deleteCollection(collectionName: String, useAsync: Boolean = true): Unit = {
    val solrServerUrl = getSolrCollectionUrl()
    val asyncId = UUID.randomUUID().toString
    val deleteCollectionUrl = s"$solrServerUrl?action=DELETE&" +
      s"name=$collectionName"
    log.info(s"deleting collection ${collectionName} if exists using ${deleteCollectionUrl}")


    if (useAsync) {
      SolrOps.makeHttpRequest(deleteCollectionUrl + s"&async=$asyncId")
      requestPolling(asyncId)
    } else {
      SolrOps.makeHttpRequest(deleteCollectionUrl)
    }
    log.info(s"listing all collections after deletion ${listCollections}")
  }

  def listCollections(): Unit = {
    val solrServerUrl = getSolrCollectionUrl()
    val listColections = solrServerUrl + "?action=LIST&wt=json"
    SolrOps.makeHttpRequest(listColections)
  }

  def createCollection(): Unit = {
    val solrServerUrl = getSolrCollectionUrl
    deleteCollection(collectionName, false)
    log.info(s"creating collection : ${collectionName} ")

    val nodeCount = SolrClusterStatus.solrLiveNodes.size
    val nameNode = solrMap("nameNode")
    val folderPrefix = solrMap("folderPrefix")
    val numShards = CollectIndices.getHdfsList(nameNode, folderPrefix,
      this.hdfsIndexFilePath).length
    if (numShards == 0) {
      throw new SolrOpsException(s"Cannot create collection with numshard count $numShards")
    }
    val replicationFactor = solrMap("replicationFactor")
    val maxShardsPerNode = ((numShards.toInt * replicationFactor.toInt) / nodeCount + 1).toString
    val asyncId = UUID.randomUUID().toString
    val createCollectionUrl = s"$solrServerUrl?action=CREATE&" +
      s"name=$collectionName" +
      s"&numShards=$numShards&" +
      s"replicationFactor=$replicationFactor&" +
      s"maxShardsPerNode=$maxShardsPerNode" +
      s"&collection.configName=$configName&" +
      s"router.name=compositeId&async=$asyncId"

    SolrOps.makeHttpRequest(createCollectionUrl)

    requestPolling(asyncId)
    val solrReponse = SolrClusterStatus.parseSolrResponse(httpTypeSolr)
    coreMap = solrReponse.map(p => (p.coreName, p.machine)).toMap
    for ((corename, ip) <- coreMap) {
      log.info(s"coreName:  ${corename} ip ${ip}"
      )
      lb.append(SolrOps.unloadCore(ip, corename, solrMap("httpTypeSolr")))
    }

    log.info(coreMap)

  }

  def waitForUnloadCores(lb: List[Future[Boolean]]): Unit = {
    var areComplete = false
    do {
      var bool = true
      log.info(s"waiting for necessary futures to complete ")
      for (f <- lb) {
        bool = f.isCompleted & bool
      }
      areComplete = bool
    }
    while (!areComplete)
    log.info(s"necessary futures completed ")

  }

  def requestPolling(asyncId: String): Unit = {
    log.info(s"polling on request for asyncId: ${asyncId}")

    while (!isReqComplete(asyncId)) {
      log.info(s"continuing to poll on request for asyncId: ${asyncId}")
      Thread.sleep(2000)
    }
    log.info(s"poll completed on request for asyncId: ${asyncId}")
  }

  def createCores(): Unit

  def deleteOldCollections(oldCollection: String): Unit


  def makeSolrCollection(aliasName: String, hdfsPath: String, workflowTime: Time): Unit = {
    this.aliasCollectionName = aliasName
    this.hdfsIndexFilePath = if (hdfsPath.last.toString == File.separator) {
      hdfsPath.slice(0, hdfsPath.length - 1)
    } else {
      hdfsPath
    }
    collectionName = s"${aliasCollectionName}_${workflowTime.getTime.toString}"
    SolrClusterStatus(solrMap("zkHosts"), solrMap("zroot"), collectionName, solrMap("httpTypeSolr"))
    val oldCollection = SolrClusterStatus.getOldCollectionMapped(aliasName)
    ZooKeeperClient(solrMap("zkHosts"))
    ZooKeeperClient.setData(s"$solrDeployerZnode/isRunning", 1.toString.getBytes)
    ZooKeeperClient.close()
    upload()
    createCollection()
    createCores()
    aliasCollection()
    deleteOldCollections(oldCollection)
    SolrClusterStatus.cloudClient.close()
    ZooKeeperClient(solrMap("zkHosts"))
    ZooKeeperClient.setData(s"$solrDeployerZnode/isRunning", 0.toString.getBytes)
    ZooKeeperClient.close()

  }

  def isReqComplete(asyncId: String): Boolean = {
    val url = getSolrCollectionUrl()
    val asyncUrl = s"$url?action=REQUESTSTATUS&requestid=$asyncId&wt=json"
    val response = SolrOps.makeHttpRequest(asyncUrl)
    val objectMapper = new ObjectMapper()
    val jsonNode = objectMapper.readTree(response)
    val asyncState = jsonNode.get("status").get("state").asText()
    log.info(s"async state for $asyncId is $asyncState")
    log.info(response)
    asyncState.equalsIgnoreCase("completed")
  }

}


object SolrOps {
  val log = Logger.getLogger(classOf[SolrOps])
  var sslContext: SSLContext = _

  def apply(mode: String,
            params: Map[String, String], sparkContext: SparkContext = null): SolrOps = {
    val solrOps = mode.toUpperCase() match {
      case "HDFS" => {
        val set = Set("appName", "zkHosts", "nameNode", "zroot", "storageDir",
          "solrConfig", "replicationFactor")
        set.foreach(p =>
          if (!params.contains(p)) {
            throw new SolrOpsException(s"Map Doesn't have ${p} map should contain ${set}")
          })
        new SolrOpsHdfs(params)
      }
      case "LOCAL" => {
        val set = Set("appName", "zkHosts", "nameNode", "solrUser",
          "folderPrefix", "zroot", "rootDirs", "storageDir", "solrConfig", "replicationFactor")
        set.foreach(p =>
          if (!params.contains(p)) {
            throw new SolrOpsException(s"Map Doesn't have ${p} map should contain ${set}")
          })
        if (!params.contains("machinePrivateKey")) {
          log.warn("missing key:machinePrivateKey  hence" +
            " assigning a default value: ~/.ssh/id_rsa")
        }
        new SolrOpsLocal(params)
      }
      case "LOCAL_API" => {
        val set = Set("appName", "zkHosts", "nameNode",
          "folderPrefix", "zroot", "solrConfig",
          "replicationFactor", "uploadEndPoint",
          "deleteEndPoint", "testEndPoint", "uploadServicePort",
          "httpType", "httpTypeSolr")
        set.foreach(p =>
          if (!params.contains(p)) {
            throw new SolrOpsException(s"Map Doesn't have ${p} map should contain ${set}")
          }
        )
        sslContext = PostZipDataAPI.getSSLContext(params)
        if(sslContext==null && params("httpType")=="https://" && params("httpTypeSolr")=="https://")
         {
           throw new SolrOpsException(s"sslContext could not be generated")
         }
        new SolrOpsLocalApi(params, sparkContext)
      }
    }
    for ((k, v) <- params) {
      log.info(s"${k}<-------->${v}")
    }
    solrOps
  }


  def unloadCore(node: String, coreName: String,
                 httpType: String = "http://"): Future[Boolean] = {
    val unloadFuture: Future[Boolean] = Future {
      log.info("unloading core")

      val client = HttpClientBuilder.create().setSslcontext(sslContext).build()

      val request = new HttpGet(s"$httpType$node/solr/admin/cores?action=UNLOAD&core=${coreName}")
      val response = client.execute(request)
      response.close()
      client.close()
      response.getStatusLine.getStatusCode == 200
    }
    unloadFuture
  }

  @throws(classOf[Exception])
  def makeHttpRequests(list: List[String], assignedTasks: Int = 20): Unit = {
    var httpthreads = if (assignedTasks > list.length) {
      list.length
    } else {
      assignedTasks
    }
    if (list.size != 0 && httpthreads != 0) {
      val pc1: ParArray[String] = ParArray
        .createFromCopy(list.toArray)
      pc1.tasksupport = new ForkJoinTaskSupport(new scala.concurrent
      .forkjoin.ForkJoinPool(httpthreads))
      try {
        pc1.foreach(url => {
          val response = makeHttpRequest(url)
          if (response != null && !response.isEmpty) {
            try {
              val objectMapper = new ObjectMapper()
              val jsonNode = objectMapper.readTree(response)
              val status = jsonNode.get("responseHeader").get("status").asInt()
              if (status != 0) {
                val e = new SolrOpsException(s"core could not be created for request: " +
                  s"$url response:$response")
                log.error(e)
                throw e
              }
            } catch {
              case e: Exception => {
                throw new SolrOpsException(s"core could not be created for request: " +
                  s"$url response:$response")
              }
            }
          }
        })
      }
      catch {
        case e: Exception =>
          throw e
      }
    }
  }

  @throws(classOf[Exception])
  def makeHttpRequest(url: String, retry: Int = 5,
                      printResponse: Boolean = true): String = {
    var responseBody: String = null
    var noError = false
    var retries = 0
    try {
      do {
        val client = HttpClientBuilder.create().setSslcontext(sslContext).build()
        val request = new HttpGet(url)
        // check for response status (should be 0)
        log.info(s"making request to url ${url}")
        if (client != null && request != null) {
          val response = client.execute(request)
          log.info(s"response status: ${response.getStatusLine} and status" +
            s" code ${response.getStatusLine.getStatusCode} ")
          responseBody = EntityUtils.toString(response.getEntity())
          if (printResponse) {
            log.info(s"responseBody: ${responseBody} for url ")
          }
          if (response.getStatusLine.getStatusCode != 200) {
            noError = true
            retries = retries + 1
          } else {
            log.info(s"attempting to make request to $url  for the retry count $retries of $retry")

            noError = false
          }
          response.close()
          client.close()
        }
      } while (retry > retries && noError)
      responseBody
    }
    catch {
      case e: Exception =>
        throw e
    }
  }
}