package com.verizon.bda.trapezium.dal.solr

import java.io.{File, IOException}

import com.verizon.bda.trapezium.dal.ZipDir
import com.verizon.bda.trapezium.dal.exceptions.SolrOpsException
import com.verizon.bda.trapezium.dal.lucene.LuceneShard
import scala.collection.mutable.{ListBuffer, Map => MMap}
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
  def deleteDirectoryViaHTTP(oldCollection: String,
                             solrHosts: Set[String], solrMap: Map[String, String]): Unit = {
    solrHosts.foreach(solrHost => {
      val url = s"${solrMap("httpType")}" +
        s"${solrHost.split(":")(0) + ":" + solrMap("uploadServicePort")}${solrMap("deleteEndPoint")}"
      SolrOps.makeHttpRequest(url, 5, true)
    })
  }

  val log = Logger.getLogger(classOf[CollectIndices])

  @throws(classOf[Exception])
  def postDataViaHTTP(sc: SparkContext, solrMap: Map[String, String],
                      hdfsIndexFilePath: String,
                      indexLocationInRoot: String,
                      coreMap: Map[String, String],
                      collectionName: String): Map[String, ListBuffer[(String, String)]] = {
    val SUFFIX = solrMap("folderPrefix").stripSuffix("/")
    val partFileMap = transformCoreMap(coreMap, solrMap)
    val partFileMapB = sc.broadcast(partFileMap)
    val partitionIds = sc.parallelize((0 until coreMap.size).toList, sc.defaultParallelism)

    val operationStatusMap = RDDUtils.mapPartitionsInternal(partitionIds,
      (indices: Iterator[Int]) => {
        indices.map((index: Int) => {
          val sparkConf = new SparkConf()
          val localDir = new File(DalUtils.getLocalDir(sparkConf))
          val hdfsPath = hdfsIndexFilePath + SUFFIX + index + "/"
          val shuffleIndexFile = new File(localDir.getAbsolutePath + SUFFIX + index)
          val shuffleIndexPath = shuffleIndexFile.toPath
          try {
            LuceneShard.copyToLocal(hdfsPath, shuffleIndexPath.toString)
            log.info(s"Copying data from deep storage: ${hdfsPath} to local shuffle:${shuffleIndexPath}")
            log.info("Before zip ****************")
            log.info(getListOfFiles(localDir.getAbsolutePath))
            log.info(getListOfFiles(localDir.getAbsolutePath + s"/part-$index"))
            ZipDir.pack(shuffleIndexPath.toString, shuffleIndexPath.toString + ".zip")
            log.info("After zip ****************")
            log.info(getListOfFiles(localDir.getAbsolutePath))
            val success = upload(shuffleIndexPath.toString + ".zip",
              s"$SUFFIX$index.zip", partFileMapB.value, collectionName)
            log.info("After upload ****************")
            log.info(getListOfFiles(localDir.getAbsolutePath))
            (s"part-$index", success)
          } catch {
            case ex: Exception => ex.printStackTrace()
              (s"part-$index", (null, false))
          }
        }
        )
      }).collect().toMap
    //   for ((host, fileList) <- map) {
    //  for ((directory, coreName) <- fileList.toList) {
    for ((_, uploadStatus) <- operationStatusMap) {
      if (!uploadStatus._2) {
        throw new SolrOpsException(s"could not successfully upload " +
          s"collection:${collectionName} data hence rolling back")
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

  def replicaNameToFolderName(solrMap: Map[String, String], replicaName: String): String = {
    val folderPrefix = solrMap("folderPrefix").stripSuffix("/")
    val tmp = replicaName.split("_")
    val partFile = folderPrefix + (tmp(tmp.length - 2).substring(5).toInt - 1)
    return partFile
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
        val path = EntityUtils.toString(response.getEntity()).split(":")(1)
        return (path, true)
      } else {
        return (null, false)
      }
    } catch {
      case e: Exception => {
        log.error(s"could not push file ${fileName} to host: ${partFileMap(fileName)}", e)
        return (null, false)
      }
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
