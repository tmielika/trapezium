package com.verizon.bda.trapezium.dal.solr

import java.io.{File, IOException}
import java.nio.file.{Files, Path, Paths}
import javax.net.ssl.SSLContext

import com.oath.auth.{KeyRefresher, Utils}
import com.verizon.bda.trapezium.dal.ZipDir
import com.verizon.bda.trapezium.dal.exceptions.SolrOpsException
import com.verizon.bda.trapezium.dal.lucene.LuceneShard
import org.apache.commons.io.FileUtils

import scala.collection.mutable.{Map => MMap, Set => MSet}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntityBuilder}
import org.apache.http.entity.mime.content.FileBody
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.log4j.Logger
import org.apache.spark.util.{DalUtils, RDDUtils}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object PostZipDataAPI {

  lazy val log = Logger.getLogger(this.getClass)

  def isApiRunningOnAllMachines(coreMap: Map[String, String],
                                solrMap: Map[String, String]): Boolean = {
    val solrHosts = getNodes(coreMap)
    log.info("inside PostZipDataAPI.isApiRunningOnAllMachines")

    for (solrHost <- solrHosts) {
      val url = s"${solrMap("httpType")}" +
        s"${solrHost.split(":")(0) + ":" + solrMap("uploadServicePort")}${solrMap("testEndPoint")}"
      try {
        val response = SolrOps.makeHttpRequest(url)
        if (response == null) {
          log.error(s"could not retrieve response from ${solrHost.split(":")(0)} for request:${url}")
          return false
        }
      } catch {
        case e: Exception =>
          log.error(s"could not connect to ${solrHost.split(":")(0)} for request:${url}", e)
          return false
      }
    }
    true
  }

  def getNodes(coreMap: Map[String, String]): Set[String] = {
    log.info("inside PostZipDataAPI.getNodes")

    coreMap.values.toSet
  }

  def deleteDirectoryViaHTTP(oldCollection: String,
                             coreMap: Map[String, String], solrMap: Map[String, String]): Unit = {

    val solrHosts = getNodes(coreMap)
    log.info(s"inside delete deleteDirectoryViaHTTP ${solrHosts.toList}")

    solrHosts.foreach(solrHost => {
      val url = s"${solrMap("httpType")}" +
        s"${solrHost.split(":")(0) + ":" + solrMap("uploadServicePort")}${solrMap("deleteEndPoint")}/$oldCollection"
      SolrOps.makeHttpRequest(url)
    })
  }


  def makeByteFromPem(file: String): Array[Byte] = {
    Files.readAllBytes(Paths.get(file))
  }

  @throws(classOf[Exception])
  def postDataViaHTTP(sc: SparkContext, solrMap: Map[String, String],
                      hdfsIndexFilePath: String,
                      coreMap: Map[String, String],
                      collectionName: String): Map[String, ListBuffer[(String, String)]] = {
    log.info("inside PostZipDataAPI.postDataViaHTTP")

    val partFileMap = transformCoreMap(coreMap, solrMap)
    val partFileMapB = sc.broadcast(partFileMap)
    val ssLContext = getSSLContext(solrMap)
    if (ssLContext == null) {
      log.error("ssl context null")
    }


    val caCertsB = sc.broadcast(makeByteFromPem(solrMap("trustStorePath")))
    val privateKeyB = sc.broadcast(makeByteFromPem(solrMap("keyPath")))
    val publicKeyB = sc.broadcast(makeByteFromPem(solrMap("certPath")))
    val trustStorePssword = getData(solrMap)

    //    val sslContextB = sc.broadcast(ssLContext)
    val partitionIds = sc.parallelize(partFileMapB.value.keySet.toList, sc.defaultParallelism).repartition(sc.defaultParallelism)
    log.info(s"inside postDataViaHTTP $partFileMap")
    val operationStatusMap = RDDUtils.mapPartitionsInternal(partitionIds,
      (partFiles: Iterator[String]) => {
        partFiles.map((partFile: String) => {
          val realPartFile = partFile.split("_")(0)
          val sparkConf = new SparkConf()
          val localDir = new File(DalUtils.getLocalDir(sparkConf) + s"/$collectionName$partFile")
          localDir.mkdirs()
          val hdfsPath = hdfsIndexFilePath + realPartFile + "/"
          val shuffleIndexFile = new File(localDir.getAbsolutePath + realPartFile)
          val shuffleIndexPath = shuffleIndexFile.toPath


          try {
            val sslContext = makeSSLContext(localDir.getAbsolutePath, caCertsB.value,
              privateKeyB.value, publicKeyB.value, trustStorePssword)
            if (sslContext == null) {
              log.error("sslcontext not intialized")
            }
            LuceneShard.copyToLocal(hdfsPath, shuffleIndexPath.toString)
            log.info(s"Copying data from deep storage: ${hdfsPath} to local shuffle:${shuffleIndexPath}")
            log.info("Before zip ****************")
            log.info(getListOfFiles(localDir.getAbsolutePath))
            log.info(getListOfFiles(localDir.getAbsolutePath + realPartFile))
            ZipDir.pack(shuffleIndexPath.toString, shuffleIndexPath.toString + ".zip")
            log.info("After zip ****************")
            log.info(getListOfFiles(localDir.getAbsolutePath))
            val success = upload(shuffleIndexPath.toString + ".zip",
              s"$realPartFile.zip", partFileMapB.value, collectionName, partFile, sslContextInput = sslContext)
            log.info("After upload ****************")
            log.info(getListOfFiles(localDir.getAbsolutePath))
            (s"$partFile", success)
          } catch {
            case ex: Exception => ex.printStackTrace()
              (partFile, (null, false))
          }
        }
        )
      }).collect().toMap
    log.info("inside PostZiClass.postDataViaHTTP before rdd map")
    log.info(operationStatusMap)
    log.info("inside PostZiClass.postDataViaHTTP after rdd map")

    for ((_, uploadStatus) <- operationStatusMap) {
      if (!uploadStatus._2) {
        throw SolrOpsException(s"could not successfully upload " +
          s"collection:$collectionName data hence rolling back")
      }
    }
    val outMap = MMap[String, ListBuffer[(String, String)]]()

    for ((replicaName, host) <- coreMap) {
      val partFile = replicaNameToFolderName(solrMap, replicaName)
      val partFilePath = operationStatusMap(partFile)._1
      if (outMap.contains(host)) {
        outMap(host).append((partFilePath, replicaName))
      } else {
        outMap(host) = new ListBuffer[(String, String)]
        outMap(host).append((partFilePath, replicaName))
      }
    }
    outMap.toMap
  }

  private def getData(solrMap: Map[String, String]) = {
    solrMap("trustStorePassword")
  }

  def transformCoreMap(coreMap: Map[String, String], solrMap: Map[String, String]): Map[String, String] = {
    log.info("inside PostZipDataAPI.transformCoreMap")

    val partFileMap = MMap[String, String]()
    for ((replicaName, host) <- coreMap) {
      val partFile = replicaNameToFolderName(solrMap, replicaName)
      val url = s"${solrMap("httpType")}" +
        s"${host.split(":")(0) + ":" + solrMap("uploadServicePort")}${solrMap("uploadEndPoint")}"
      partFileMap(partFile) = url
    }
    partFileMap.toMap
  }

  //  replicaName = qa_clarobr150_gemini_collection_1538182429399_shard3_replica1

  def replicaNameToFolderName(solrMap: Map[String, String], replicaName: String): String = {
    log.info("inside PostZipDataAPI.replicaNameToFolderName")

    val folderPrefix = solrMap("folderPrefix").stripSuffix("/")
    val tmp = replicaName.split("_")
    val partFile = folderPrefix + (tmp(tmp.length - 2).substring(5).toInt - 1) + "_" + tmp(tmp.length - 1).charAt(7)
    partFile
  }

  def getMd5(file: File): String = {
    import java.io.FileInputStream
    val fis = new FileInputStream(file)
    org.apache.commons.codec.digest.DigestUtils.md5Hex(fis)
  }

  def makeSSLContext(path: String, caCerts: Array[Byte],
                     privateKey: Array[Byte], publicKey: Array[Byte], trustPsWord: String): SSLContext = {
    val caCertsPath = path + "/caCerts.jks"
    val privateKeyPath = path + "/privateKey.pem"
    val publicKeyPath = path + "/publicKey.pem"
    log.info("caCertsPath: {}, publicKeyPath: {}, privateKeyPath:{}, trustStorePassword{}",
      caCertsPath, publicKeyPath, privateKeyPath, trustPsWord)
    FileUtils.writeByteArrayToFile(new File(caCertsPath), caCerts)
    FileUtils.writeStringToFile(new File(privateKeyPath), new String(privateKey), "UTF-8")
    FileUtils.writeStringToFile(new File(publicKeyPath), new String(publicKey), "UTF-8")
    createSSLContext(caCertsPath, publicKeyPath, privateKeyPath, trustPsWord)
  }

  def upload(zipFile: String, fileName: String,
             partFileMap: Map[String, String],
             collectionName: String, partFile: String, sslContextInput: SSLContext = null): (String, Boolean) = {
    log.info("inside PostZipDataAPI.upload")
    log.info(partFileMap)
    val inFile = new File(zipFile)
    val md5 = getMd5(inFile)
    log.info(zipFile)
    //    val host = partFileMap(fileName)
    val fileBody = new FileBody(inFile, ContentType.DEFAULT_BINARY, fileName)
    val builder = MultipartEntityBuilder.create
    builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
    builder.addPart("uploadedFile", fileBody)
    //    builder.addPart("collectionName", collectionName)
    val entity = builder.build
    val request = new HttpPost(partFileMap(partFile))
    request.setHeader("md5", md5)
    request.setHeader("collectionName", collectionName)
    request.setEntity(entity)
    val client = HttpClientBuilder.create.setSslcontext(sslContextInput).build
    var response: CloseableHttpResponse = null
    try {
      response = client.execute(request)
      val responseString = EntityUtils.toString(response.getEntity, "UTF-8")

      log.info(response.getStatusLine)
      log.info(responseString)
      // ToDo
      if (response.getStatusLine.getStatusCode == 200) {
        val path = responseString.split(":")(1)
        (path, true)
      } else {
        (null, false)
      }
    } catch {
      case e: Throwable =>
        log.error(s"could not push file $fileName to host: ${partFileMap(partFile)}", e)
        (null, false)
    } finally {
      response.close()
      client.close()
    }
  }


  def getListOfFiles(dir: String): List[File] = {
    log.info("inside PostZipDataAPI.getListOfFiles")

    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }


  def getSSLContext(map: Map[String, String]): SSLContext = {

    //      val caCerts = map("trustStorePath")
    //      val privateKey = map("keyPath")
    //      val publicKey = map("certPath")
    val privateKey = map("keyPath")
    val publicKey = map("certPath")
    val trustStorePath = map("trustStorePath")
    val trustStorePssWord = getData(map)
    //                val keyRefresher: KeyRefresher = Utils.generateKeyRefresher(trustStorePath, trustStorePassword,
    //                    certPath, keyPath)
    createSSLContext(trustStorePath, publicKey, privateKey, trustStorePssWord)

  }

  def createSSLContext(caCerts: String, publicKey: String,
                       privateKey: String, trustStorePssword: String): SSLContext = {
    try {
      //    val keyRefresher: KeyRefresher = Utils.generateKeyRefresherFromCaCert(caCerts, publicKey, privateKey)
      val keyRefresher: KeyRefresher = Utils.generateKeyRefresher(caCerts, trustStorePssword,
        publicKey, privateKey)
      // Default refresh period is every hour.
      keyRefresher.startup()
      // Can be adjusted to use other values in milliseconds.
      // keyRefresher.startup(900000);
      Utils.buildSSLContext(keyRefresher.getKeyManagerProxy,
        keyRefresher.getTrustManagerProxy())
    }
    catch {
      case th: Throwable =>
        log.error("sslcontext not formed", th)
        null
    }
  }

}
