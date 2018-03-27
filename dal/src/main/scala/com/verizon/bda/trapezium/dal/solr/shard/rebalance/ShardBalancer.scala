package com.verizon.bda.trapezium.dal.solr.shard.rebalance

import java.io.File
import java.util

import com.typesafe.config.{Config, ConfigFactory}
import com.verizon.bda.trapezium.dal.solr.{CollectIndices, SolrClusterStatus, SolrOps, SolrOpsLocal}
import com.verizon.bda.trapezium.dal.util.zookeeper.ZooKeeperClient
import org.apache.log4j.Logger
import org.json.JSONObject
import scopt.OptionParser

import scala.collection.mutable.{Map => MMap}
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object ShardBalancer {
  val solrDeployerZnode = "/solrDeployer"


  lazy val log = Logger.getLogger(classOf[SolrOps])

  def getDeleterReplicaUrl(solrNode: String, collection: String,
                           coreNode: String, shardId: String): String = {
    s"http://$solrNode/solr/admin/collections?action=DELETEREPLICA" +
      s"&collection=$collection&replica=$coreNode&shard=$shardId&wt=json"
  }

  //  def getCoreCreateURL(host: String, collectionName: String, configName: String,
  //                       coreName: String, directory: String, shardId: String): String = {
  //    s"http://$host/solr/admin/cores?" +
  //      "action=CREATE&" +
  //      s"collection=${collectionName}&" +
  //      s"collection.configName=${configName}&" +
  //      s"name=${coreName}&" +
  //      s"dataDir=${directory}&" +
  //      s"shard=$shardId&" +
  //      s"wt=json&indent=true"
  //  }
  case class FailureShardInfomation(deleteUrls: List[String], coreMap: Map[String, String],
                                    configName: String, failureNodes: String)

  def collectAllFailureNodes(collection: String, failureRepeat: Int):
  FailureShardInfomation = {
    val collectionJson = new JSONObject(SolrClusterStatus.getClusterStatus(collection, true))
      .getJSONObject("cluster")
      .getJSONObject("collections")
      .getJSONObject(collection)
    val collectionConfig = collectionJson.getString("configName")
    val shardMap = collectionJson.getJSONObject("shards")
      .toMap.asScala.toMap
    val solrLiveNodes = SolrClusterStatus.solrLiveNodes
    val coreMap = scala.collection.mutable.Map[String, String]()
    val lb = new ListBuffer[String]
    val nodeSet = scala.collection.mutable.Set[String]()
    var count = 0
    val aliveShardSet = scala.collection.mutable.Set[String]()

    for ((shardId, v) <- shardMap) {
      val shard = v.asInstanceOf[util.HashMap[String, util.HashMap[String,
        util.HashMap[String, String]]]]
      val replicas = shard.get("replicas")
      replicas.asScala.toMap.
        foreach(p => {
          var prev = 0
          if (p._2.get("state") == "active") {
            aliveShardSet.add(shardId)
          }
        })
    }
    for ((shardId, v) <- shardMap) {
      val shard = v.asInstanceOf[util.HashMap[String, util.HashMap[String,
        util.HashMap[String, String]]]]
      val solrNode = SolrClusterStatus.solrLiveNodes.head

      val replicas = shard.get("replicas")
      val set = scala.collection.mutable.Set[String]()
      replicas.asScala.toMap.
        foreach(p => {
          var prev = 0

          if (p._2.get("state") != "active") {
            count = (count + 1 + Random.nextInt() *
              Random.nextInt(solrLiveNodes.size)) % solrLiveNodes.size
            if (prev == count) {
              count = (count + 1) % solrLiveNodes.size
            }
            if (count < 0) {
              count = 0
            }
            // sample_daily_1520966909517_shard3_replica1
            val replicaName = p._2.get("core") + failureRepeat
            val coreNodeName: String = p._1 // core_node5
            val nodeName: String = p._2.get("node_name") // 132.197.10.29:8986_solr
            val solrNo = solrLiveNodes(count)
            if (!aliveShardSet.contains(shardId)) {
              coreMap(replicaName) = solrNo
              aliveShardSet.add(shardId)
            }
            log.info(s"node to be used for $replicaName is" +
              s" ${solrLiveNodes(count)} from $solrLiveNodes")
            nodeSet.add(nodeName)
            val deleteUrl = getDeleterReplicaUrl(solrNode, collection, coreNodeName, shardId)
            lb.append(deleteUrl)
          }
          else {
            set.add(p._2.get("core"))
          }
          prev = count
        })
    }
    FailureShardInfomation(lb.toList, coreMap.toMap, collectionConfig,
      nodeSet.toList.sorted.mkString(","))
  }

  case class ShardBalancer(configDir: String = null, configFile: String = null, waitInmis: Int = 0)

  def findKeyOfMap(value: String, map: Map[String, String]): String = {
    map.find({ case (a, b) => b == value }).get._1
  }

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
      opt[Int]("waitInmins")
        .text(s"wait time in mins needed for thread restart ")
        .required
        .action((x, c) => c.copy(waitInmis = x))
    }
    while (true) {
      val shardBalancer = parser.parse(args, ShardBalancer()).get
      val configDir: String = shardBalancer.configDir
      val configFile: String = shardBalancer.configFile
      val config: Config = readConfigs(configDir, configFile)
      val zkList = config.getString("solr.zkhosts")
      val zroot = config.getString("solr.zroot")
      val nameNode = config.getString("solr.nameNode")
      ZooKeeperClient(zkList)
      val zk = ZooKeeperClient
      val deployerUsage = zk.getData(s"$solrDeployerZnode/isRunning")
      zk.close()
      if (deployerUsage.toInt != 1) {
        SolrClusterStatus(zkList, zroot, "")
        val aliasMap = SolrClusterStatus.getCollectionAliasMap()
        if (aliasMap.keySet.isEmpty) {
          throw new Exception("no aliases found")
        }
        val collections: List[String] = aliasMap.values.toList
        collections.foreach(collection => {
          val aliasCollection = findKeyOfMap(collection, aliasMap)
          ZooKeeperClient(zkList)
          var collectionFailurecount = ZooKeeperClient.getData(s"$solrDeployerZnode/" +
            s"$aliasCollection/collectionFailurecount").toInt
          collectionFailurecount = collectionFailurecount + 1
          val failureShards = collectAllFailureNodes(collection, collectionFailurecount)
          ZooKeeperClient.setData(s"$solrDeployerZnode/" +
            s"$aliasCollection/collectionFailurecount",
            (collectionFailurecount + "").getBytes())
          ZooKeeperClient.close()
          if (failureShards.deleteUrls.length > 0) {
            ZooKeeperClient(zkList)
            val indexLocation = ZooKeeperClient.getData(s"$solrDeployerZnode/" +
              s"$aliasCollection/indicesPath")
            val hdfsIndexFilePath = ZooKeeperClient.getData(s"$solrDeployerZnode/" +
              s"$aliasCollection/hdfsPath")
            ZooKeeperClient.close()
            // DeleteReplicas
            val deleteReplicaUrls = failureShards.deleteUrls
            SolrOps.makeHttpRequests(deleteReplicaUrls)
            // MoveData to assigned Nodes

            val solrNodeUser = config.getString("solr.node_ssh_user")
            //      val machinePrivateKey = config.getString("machinePrivateKey")
            val rootDirs = config.getString("solr.disks")
            val folderPrefix = config.getString("solr.index_folder_prefix")
            val solrMap = Map(
              "solrUser" -> solrNodeUser,
              "rootDirs" -> rootDirs,
              "folderPrefix" -> folderPrefix,
              "appName" -> "",
              "numHTTPTasks" -> "20",
              "nameNode" -> nameNode
            )

            val ipShardMap = CollectIndices.moveFilesFromHdfsToLocal(
              solrMap,
              hdfsIndexFilePath,
              indexLocation, failureShards.coreMap, true
            )
            val sol = new SolrOpsLocal(solrMap)
            sol.hdfsIndexFilePath = hdfsIndexFilePath
            sol.createCoresOnSolr(ipShardMap, collection,
              failureShards.configName)

          }

        })

      }
      Thread.sleep(1000L * 60 * shardBalancer.waitInmis)
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