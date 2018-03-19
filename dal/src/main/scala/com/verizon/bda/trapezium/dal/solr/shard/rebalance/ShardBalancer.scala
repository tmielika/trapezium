package com.verizon.bda.trapezium.dal.solr.shard.rebalance

import java.io.File
import java.util

import com.typesafe.config.{Config, ConfigFactory}
import com.verizon.bda.analytics.api.dao.zk.ZooKeeperClient
import com.verizon.bda.trapezium.dal.solr.{CollectIndices, SolrClusterStatus, SolrOps}
import org.apache.log4j.Logger
import org.json.JSONObject
import scopt.OptionParser

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object ShardBalancer {

  case class FailureShard(shardName: String, coreName: String, nodeName: String,
                          hdfsIndexPath: String, var createUrl: String, deleteUrl: String)

  lazy val log = Logger.getLogger(classOf[SolrOps])

  def getDeleterReplicaUrl(solrNode: String, collection: String, coreNode: String, shardId: String): String = {
    s"http://$solrNode/solr/admin/collections?action=DELETEREPLICA" +
      s"&collection=$collection&replica=$coreNode&shard=$shardId&wt=json"
  }

  def getCoreCreateURL(host: String, collectionName: String, configName: String,
                       coreName: String, directory: String, shardId: String): String = {
    s"http://$host/solr/admin/cores?" +
      "action=CREATE&" +
      s"collection=${collectionName}&" +
      s"collection.configName=${configName}&" +
      s"name=${coreName}&" +
      s"dataDir=${directory}&" +
      s"shard=$shardId&" +
      s"wt=json&indent=true"
  }

  case class

  def collectAllFailureNodes(collection: String): List[FailureShard] = {
    val collectionJson = new JSONObject(SolrClusterStatus.getClusterStatus(collection, true))
      .getJSONObject("cluster")
      .getJSONObject("collections")
      .getJSONObject(collection)
    val collectionConfig = collectionJson.getString("configName")
    val shardMap = collectionJson.getJSONObject("shards")
      .toMap.asScala.toMap
    val lb: ListBuffer[FailureShard] = new ListBuffer[FailureShard]
    val solrNodes = SolrClusterStatus.solrLiveNodes
    val lb1 = new ListBuffer[]
    for ((shardId, v) <- shardMap) {

      val shard = v.asInstanceOf[util.HashMap[String, util.HashMap[String,
        util.HashMap[String, String]]]]
      val solrNode = SolrClusterStatus.solrLiveNodes.head
      var count = 0
      val replicas = shard.get("replicas")
      replicas.asScala.toMap.
        foreach(p => {
          if (p._2.get("state") != "active") {

            val shardName: String = shardId

            count = (count + 1) % solrNodes.size
            val coreName = p._2.get("core")
            val createUrl = getCoreCreateURL(solrNodes(count), collection, collectionConfig, coreName, directory, shardName)
            val coreNodeName: String = p._1
            val nodeName: String = p._2.get("nodeName")
            val hdfsIndexPath: String =
            val deleteUrl = getDeleterReplicaUrl(solrNode, collection, coreNodeName, shardId)
            deleteUrl: String
            lb.append(FailureShard(shardId, p._1, p._2.get("node_name").split("_")(0)))
          }
        })
    }
    lb.toList
  }

  case class ShardBalancer(configDir: String = null, configFile: String = null)

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[ShardBalancer]("ShardBalancer") {
      head("ShardBalancer service for reducing solr downtime ")
      opt[String]("config")
        .text(s"local config directory path")
        .optional
        .action((x, c) => c.copy(configDir = x))
      opt[String]("file")
        .text(s"config file path where all the configurations ")
        .required
        .action((x, c) => c.copy(configFile = x))
    }
    val shardBalancer = parser.parse(args, ShardBalancer()).get
    val configDir: String = shardBalancer.configDir
    val configFile: String = shardBalancer.configFile
    val config = readConfigs(configDir, configFile)
    val zkList = config.getString("solr.zkhosts")
    val zroot = config.getString("solr.zroot")

    ZooKeeperClient(zkList)
    val zk = ZooKeeperClient
    val deployerUsage = zk.getData(config.getString("solr.zk.deployerpath"))
    if (deployerUsage.toInt != 1) {
      SolrClusterStatus(zkList, zroot, "")
      val clusterState = SolrClusterStatus.getClusterStatus("", false)
      SolrClusterStatus.getCollectionAliasMap()
      val aliases: List[String] = SolrClusterStatus.getCollectionAliasMap().keySet.toList
      val collections: List[String] = SolrClusterStatus.getCollectionAliasMap().values.toList
      val failureShards: List[FailureShard] = collections.foreach(collection =>
        collectAllFailureNodes(collection)
      // DeleteReplicas
      val deleteReplicaUrls = failureShards.map(_.deleteUrl)
      SolrOps.makeHttpRequests(deleteReplicaUrls)
      // MoveData to assigned Nodes
      //      val solrUser = config.getString("solr.user")

      val solrNodeUser = config.getString("solr.node_ssh_user")
      //      val machinePrivateKey = config.getString("machinePrivateKey")
      val rootDirs = config.getString("solr.disks")
      val folderPrefix = config.getString("solr.index_folder_prefix")

      val solrMap = Map("solrNodeUser" -> solrNodeUser,
        "rootDirs" -> rootDirs,
        "solrNodeUser" -> solrNodeUser
      )
      val hdfsIndexFilePath =  zk.getData(""
      val indexLocationInRoot = ""
      (shardId, host)
      <- coreMap
      val coreMap: Map[String, String] = _
      CollectIndices.moveFilesFromHdfsToLocal(
        solrMap,
        hdfsIndexFilePath,
        indexLocationInRoot, coreMap, true
      )
      val makeColreUrls = failureShards.map(_.createUrl)
      // Create cores
      SolrOps.makeHttpRequests(makeColreUrls)
    }

  }

  def readConfigs(configDir: String, configFile: String): Config = {
    if (configDir == null) {
      log.info(s"Reading config file ${configFile} from jar")
      ConfigFactory.load(configFile)
    } else {
      log.info(s"Reading config file ${configFile} from ${configDir}")
      ConfigFactory.parseFile(new File(s"${configDir}/$configFile"))
    }
  }
}
