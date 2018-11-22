package com.verizon.bda.trapezium.dal.solr.shard.rebalance

import java.io.File
import java.util

import com.typesafe.config.{Config, ConfigFactory}
import com.verizon.bda.trapezium.dal.solr._
import com.verizon.bda.trapezium.dal.util.zookeeper.ZooKeeperClient
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import scopt.OptionParser

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Random

object ShardBalancerAPI {
  val solrDeployerZnode = "/solrDeployer"


  lazy val log = Logger.getLogger(this.getClass)

  def getDeleterReplicaUrl(solrNode: String, collection: String,
                           coreNode: String, shardId: String,
                           skipCoreNode: Boolean = false): String = {
    if (skipCoreNode) {
      s"http://$solrNode/solr/admin/collections?action=DELETEREPLICA" +
        s"&collection=$collection&shard=$shardId&wt=json"
    } else {
      s"http://$solrNode/solr/admin/collections?action=DELETEREPLICA" +
        s"&collection=$collection&replica=$coreNode&shard=$shardId&wt=json"
    }
  }

  case class FailureShardInfomation(deleteUrls: List[String], coreMap: Map[String, String],
                                    configName: String, failureNodes: String)

  def collectAllFailureNodes(collection: String, failureRepeat: Int):
  FailureShardInfomation = {
    val collectionJson = new JSONObject(SolrClusterStatus.getClusterStatus(collection))
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
      log.info(s"shardId $shardId replica map " + shard.asScala.toMap)
      val replicas: util.HashMap[String, util.HashMap[String, String]] = shard.get("replicas")
      log.info(s"replica set  $replicas")

      val set = scala.collection.mutable.Set[String]()
      if (replicas.isEmpty || replicas.size() == 0) {
        val solrNo = solrLiveNodes(count)
        val replicaName = s"${collection}_${shardId}_replica$failureRepeat"
        coreMap(replicaName) = solrNo
        count = (count + 1) % solrLiveNodes.size
        if (count >= solrLiveNodes.size) {
          count = 0
        }
      }
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

  case class ShardBalancer(configDir: String = null, configFile: String = null,
                           waitInMins: Int = 0, collections: String = null)

  def findKeyOfMap(value: String, map: Map[String, String]): String = {
    map.find({ case (a, b) => b == value }).get._1
  }

  def inputParser(): OptionParser[ShardBalancer] = {
    new OptionParser[ShardBalancer]("ShardBalancer") {
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
        .action((x, c) => c.copy(waitInMins = x))
      opt[String]("collections")
        .text(s"collections to watch at provide aliases")
        .required
        .action((x, c) => c.copy(collections = x))
    }
  }

  def main(args: Array[String]): Unit = {
    val parser = inputParser()
    while (true) {
      val sparkConf = new SparkConf().setAppName("Solr HA with API")
      val spark = SparkSession.builder().config(sparkConf).getOrCreate()
      val shardBalancer = parser.parse(args, ShardBalancer()).get
      val configDir: String = shardBalancer.configDir
      val configFile: String = shardBalancer.configFile
      val collections: List[String] = shardBalancer.collections.split(",").toList
      val config: Config = readConfigs(configDir, configFile)
      val zkList = config.getString("solr.zkhosts")
      val zroot = config.getString("solr.zroot")
      haStart(zkList, zroot, config, collections, spark.sparkContext)
      Thread.sleep(1000L * 60 * shardBalancer.waitInMins)
    }
  }

  def haStart(zkList: String, zroot: String, config: Config, collections: List[String], sparkContext: SparkContext,
              retryCount: Int = 5): Unit = {
    try {
      ZooKeeperClient(zkList)
      val deployerUsage = ZooKeeperClient.getData(s"$solrDeployerZnode/isRunning")
      ZooKeeperClient.close()
      if (deployerUsage.toInt != 1) {
        SolrClusterStatus(zkList, zroot, "")
        val aliasMap = SolrClusterStatus.getCollectionAliasMap()
        if (aliasMap.keySet.isEmpty) {
          throw new Exception("no aliases found")
        }
        val liveCollections: List[String] = aliasMap.keySet.intersect(collections.toSet).toList
          .map(p => aliasMap(p))
        log.info(s"collections being watched are $liveCollections")
        liveCollections.foreach(collection => {
          val aliasCollection = findKeyOfMap(collection, aliasMap)
          ZooKeeperClient(zkList)
          var collectionFailurecount = ZooKeeperClient.getData(s"$solrDeployerZnode/" +
            s"$aliasCollection/collectionFailurecount").toInt
          collectionFailurecount = collectionFailurecount + 1
          val failureShards = collectAllFailureNodes(collection, collectionFailurecount)
          val actualLiveNodes = ZooKeeperClient.getData(s"$solrDeployerZnode/" +
            s"$aliasCollection/assignedLiveNodes").toDouble
          val allowedFailure = config.getInt("solr.clusterCapacity") * 0.01
          require(allowedFailure < 1, "solr.clusterCapacity" +
            " value should be less than 100")
          if (SolrClusterStatus.solrLiveNodes.length < actualLiveNodes * (1 - allowedFailure)) {
            log.error(s"HA cannot work as the number of available nodes" +
              s" ${SolrClusterStatus.solrLiveNodes.length} is less than " +
              s"percent of node failure allowed  $allowedFailure  " +
              s"on actual number of nodes $actualLiveNodes")
            return
          }
          ZooKeeperClient.close()
          if (failureShards.coreMap.keySet.nonEmpty) {
            ZooKeeperClient(zkList)
            ZooKeeperClient.setData(s"$solrDeployerZnode/" +
              s"$aliasCollection/collectionFailurecount",
              (collectionFailurecount + "").getBytes())
            log.info(s"Number of times Solr Collection failed is $collectionFailurecount")

            val indexLocation = ZooKeeperClient.getData(s"$solrDeployerZnode/" +
              s"$aliasCollection/indicesPath")
            val hdfsIndexFilePath = ZooKeeperClient.getData(s"$solrDeployerZnode/" +
              s"$aliasCollection/hdfsPath")
            ZooKeeperClient.close()
            // DeleteReplicas
            val deleteReplicaUrls = failureShards.deleteUrls
            SolrOps.makeHttpRequests(deleteReplicaUrls)

            val folderPrefix = config.getString("solr.index_folder_prefix")
            val uploadServicePort = config.getString("solr.uploadServicePort")
            val httpType = config.getString("solr.httpType")
            val uploadEndPoint = config.getString("solr.uploadEndPoint")
            val httpTypeSolr = config.getString("solr.httpTypeSolr")

            val solrMap = Map(
              "folderPrefix" -> folderPrefix,
              "storageDir" -> indexLocation.concat("/"),
              "appName" -> "",
              "uploadServicePort" -> uploadServicePort,
              "httpType" -> httpType,
              "uploadEndPoint" -> uploadEndPoint,
              "httpTypeSolr" -> httpTypeSolr
            )

            //            val ipShardMap = CollectIndices.moveFilesFromHdfsToLocal(
            //              solrMap,
            //              hdfsIndexFilePath,
            //              indexLocation, failureShards.coreMap, true
            //            )
            //            val sol = new SolrOpsLocal(solrMap)
            //            sol.hdfsIndexFilePath = hdfsIndexFilePath
            //            sol.createCoresOnSolr(ipShardMap, collection,
            //              failureShards.configName)

            val isRunning = PostZipDataAPI.isApiRunningOnAllMachines(failureShards.coreMap, solrMap)
            require(isRunning, "all the Api nodes are not running")
            // moved data to the location on solr
            val ipShardMap =
              PostZipDataAPI.postDataViaHTTP(sparkContext, solrMap, hdfsIndexFilePath,
                failureShards.coreMap, collection)

            val sol = new SolrOpsLocalApi(solrMap, sparkContext)
            sol.hdfsIndexFilePath = hdfsIndexFilePath
            sol.createCoresOnSolr(ipShardMap, collection,
              failureShards.configName)
          }
        })
      }
    }
    catch {
      case e: Throwable => {
        log.warn("trying to restart the ha as there was exception", e)
        log.info("Retrying HA")
        if (retryCount == 0) {
          log.error(s"quitting ${classOf[ShardBalancer]} after 5 retries with ", e)
          System.exit(-1)
        }
        haStart(zkList, zroot, config, collections, sparkContext, retryCount - 1)
      }
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