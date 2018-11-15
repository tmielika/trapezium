package com.verizon.bda.trapezium.dal.solr

import java.io.{File, IOException}

import com.verizon.bda.trapezium.dal.ZipDir
import com.verizon.bda.trapezium.dal.exceptions.SolrOpsException
import com.verizon.bda.trapezium.dal.lucene.LuceneShard
import scala.collection.mutable.{Set => MSet, Map => MMap}
import org.apache.http.client.methods.HttpPost
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
  def isApiRunningOnAllMachines(solrHosts: Set[String], solrMap: Map[String, String]): Boolean = {
    for (solrHost <- solrHosts) {
      val url = s"${solrMap("httpType")}" +
        s"${solrHost.split(":")(0) + ":" + solrMap("uploadServicePort")}${solrMap("testEndPoint")}"
      try {
        val response = SolrOps.makeHttpRequest(url, 5, true)
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


  def deleteDirectoryViaHTTP(oldCollection: String,
                             solrHosts: Set[String], solrMap: Map[String, String]): Unit = {
    solrHosts.foreach(solrHost => {
      val url = s"${solrMap("httpType")}" +
        s"${solrHost.split(":")(0) + ":" + solrMap("uploadServicePort")}${solrMap("deleteEndPoint")}"
      SolrOps.makeHttpRequest(url)
    })
  }

  val log = Logger.getLogger(classOf[CollectIndices])

  @throws(classOf[Exception])
  def postDataViaHTTP(sc: SparkContext, solrMap: Map[String, String],
                      hdfsIndexFilePath: String,
                      coreMap: Map[String, String],
                      collectionName: String): Map[String, ListBuffer[(String, String)]] = {
    val partFileMap = transformCoreMap(coreMap, solrMap)
    val partFileMapB = sc.broadcast(partFileMap)
    val partitionIds = sc.parallelize(partFileMapB.value.keySet.toList, sc.defaultParallelism)
    val operationStatusMap = RDDUtils.mapPartitionsInternal(partitionIds,
      (partFiles: Iterator[String]) => {
        partFiles.map((partFile: String) => {
          val realPartFile = partFile.split("_")(0)
          val sparkConf = new SparkConf()
          val localDir = new File(DalUtils.getLocalDir(sparkConf))
          val hdfsPath = hdfsIndexFilePath + realPartFile + "/"
          val shuffleIndexFile = new File(localDir.getAbsolutePath + realPartFile)
          val shuffleIndexPath = shuffleIndexFile.toPath
          try {
            LuceneShard.copyToLocal(hdfsPath, shuffleIndexPath.toString)
            log.info(s"Copying data from deep storage: ${hdfsPath} to local shuffle:${shuffleIndexPath}")
            log.info("Before zip ****************")
            log.info(getListOfFiles(localDir.getAbsolutePath))
            log.info(getListOfFiles(localDir.getAbsolutePath + realPartFile))
            ZipDir.pack(shuffleIndexPath.toString, shuffleIndexPath.toString + ".zip")
            log.info("After zip ****************")
            log.info(getListOfFiles(localDir.getAbsolutePath))
            val success = upload(shuffleIndexPath.toString + ".zip",
              s"$realPartFile.zip", partFileMapB.value, collectionName)
            log.info("After upload ****************")
            log.info(getListOfFiles(localDir.getAbsolutePath))
            (s"$realPartFile", success)
          } catch {
            case ex: Exception => ex.printStackTrace()
              (partFile, (null, false))
          }
        }
        )
      }).collect().toMap
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

  def transformCoreMap(coreMap: Map[String, String], solrMap: Map[String, String]): Map[String, String] = {
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
    val folderPrefix = solrMap("folderPrefix").stripSuffix("/")
    val tmp = replicaName.split("_")
    val partFile = folderPrefix + (tmp(tmp.length - 2).substring(5).toInt - 1) + "_" + tmp(tmp.length - 1).charAt(7)
    partFile
  }

  def upload(zipFile: String, fileName: String,
             partFileMap: Map[String, String], collectionName: String): (String, Boolean) = {
    val inFile = new File(zipFile)
    log.info(zipFile)
    //    val host = partFileMap(fileName)
    val fileBody = new FileBody(inFile, ContentType.DEFAULT_BINARY, fileName)
    val builder = MultipartEntityBuilder.create
    builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
    builder.addPart("uploadedFile", fileBody)
    //    builder.addPart("collectionName", collectionName)
    val entity = builder.build
    val request = new HttpPost(partFileMap(fileName))
    request.setHeader("collectionName", collectionName)
    request.setEntity(entity)
    val client = HttpClientBuilder.create.build
    try {
      val response = client.execute(request)
      val responseString = EntityUtils.toString(response.getEntity, "UTF-8")

      log.info(response.getStatusLine)
      log.info(responseString)
      if (response.getStatusLine.getStatusCode == 200) {
        val path = EntityUtils.toString(response.getEntity).split(":")(1)
        (path, true)
      } else {
        (null, false)
      }
    } catch {
      case e: Throwable =>
        log.error(s"could not push file $fileName to host: ${partFileMap(fileName)}", e)
        (null, false)
    }
  }


  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }


}
