package com.verizon.bda.trapezium.dal.solr

import java.net.URI

import com.verizon.bda.trapezium.dal.util.zookeeper.ZooKeeperClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by venkatesh on 7/12/17.
  */
class SolrOpsLocal(solrMap: Map[String, String]) extends SolrOps(solrMap: Map[String, String]) {

  override lazy val log: Logger = Logger.getLogger(classOf[SolrOpsLocal])
  lazy val indexLocationInRoot: String = solrMap("storageDir") + collectionName

  lazy val map: Map[String, ListBuffer[(String, String)]] = getHostToFileMap()

  def getHostToFileMap(): Map[String, ListBuffer[(String, String)]] = {
    try {
      CollectIndices.
        moveFilesFromHdfsToLocal(solrMap,
          hdfsIndexFilePath, indexLocationInRoot, coreMap)
    }
    catch {
      case e: Exception =>
        rollBackCollections(collectionName)
        log.error(s"could not create collection $collectionName", e)
        null
    }
  }

  def createCores(): Unit = {
    log.info("inside create create cores")
    log.info(map)
    createCoresOnSolr(map, collectionName, configName)
  }

  /**
    *
    * @param map 's key is the host and value is the tuple of directory and corename
    * @param collectionName
    * @param configName
    */
  @throws(classOf[Exception])
  def createCoresOnSolr(map: Map[String, ListBuffer[(String, String)]],
                        collectionName: String, configName: String): Unit = {
    try {
      log.info("inside create cores")
      val list = new ListBuffer[String]
      waitForUnloadCores(lb.toList)
      for ((host, fileList) <- map) {
        for ((directory, coreName) <- fileList.toList) {
          val id = directory.split("-").last.toInt + 1
          val url = s"$httpTypeSolr$host/solr/admin/cores?" +
            "action=CREATE&" +
            s"collection=$collectionName&" +
            s"collection.configName=$configName&" +
            s"name=$coreName&" +
            s"dataDir=$directory&" +
            s"shard=shard$id&" +
            s"wt=json&indent=true"
          list.append(url)
        }
      }
      log.info(list.toList)
      SolrOps.makeHttpRequests(list.toList, solrMap("numHTTPTasks").toInt)
      if (!makeSanityCheck(collectionName, map)) {
        rollBackCollections(collectionName)
        log.error(s"sanity check failed and rolling back " +
          s"the creation of collection $collectionName")
        System.exit(1)
      }
    }
    catch {
      case e: Exception =>
        log.error(s"could create  $collectionName")
        rollBackCollections(collectionName)
    }
  }

  @throws(classOf[Exception])
  def makeSanityCheck(collectionName: String,
                      map: Map[String, ListBuffer[(String, String)]]): Boolean = {
    for ((host, fileList) <- map) {
      for ((directory, coreName) <- fileList.toList) {
        val id: Int = directory.split("-").last.toInt
        val url = s"$httpTypeSolr$host/solr/admin/cores?" +
          "action=STATUS&" +
          s"core=$coreName&" +
          s"wt=json"
        val response = SolrOps.makeHttpRequest(url)
        val obj = new JSONObject(response)
        val sizeOnLocal = obj.getJSONObject("status")
          .getJSONObject(coreName)
          .getJSONObject("index")
          .getLong("sizeInBytes")
        val nameNode = solrMap("nameNode")
        val folderPrefix = solrMap("folderPrefix")
        val partFileNum = id
        val hdfsDataLocation = hdfsIndexFilePath + folderPrefix + partFileNum
        val sizeOnHdfs = getShardSizeOnHdfs(nameNode, hdfsDataLocation)
        log.info(s"shard$id on local $sizeOnLocal and size on hdfs $sizeOnHdfs")

//        if (Math.abs(sizeOnLocal - sizeOnHdfs) != 0) {
//          log.warn(s"size of shard$id on hdfs and local didn't match hence " +
//            s"initiating a roll back")
//          return false
//        }
      }
    }
    true
  }

  def getShardSizeOnHdfs(nameNode: String, filePath: String): Long = {
    log.info(s"verifying the size of $filePath")
    val configuration: Configuration = new Configuration()
    configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    // 2. Get the instance of the HDFS
    val hdfs = FileSystem.get(new URI(s"hdfs://${
      nameNode
    }"), configuration)
    // 3. Get the metadata of the desired directory
    val hdfsFilePath = new Path(s"hdfs://${
      nameNode
    }" + filePath)
    hdfs.getContentSummary(hdfsFilePath).getLength()
  }

  override def deleteOldCollections(oldCollection: String): Unit = {
    if (oldCollection != null) {
      deleteCollection(oldCollection, false)
      val oldCollectionDirectory = solrMap("storageDir") + oldCollection
      CollectIndices.deleteDirectory(oldCollectionDirectory, solrMap("rootDirs").split(","))
    }
    CollectIndices.closeSessions()
  }

  def rollBackCollections(collection: String): Unit = {
    log.warn(s"rollling back collection $collection")
    deleteOldCollections(collection)
    ZooKeeperClient(solrMap("zkHosts"))
    ZooKeeperClient.setData(s"$solrDeployerZnode/isRunning", 0.toString.getBytes)
    ZooKeeperClient.close()

  }
}
