package com.verizon.bda.trapezium.dal.solr

import org.apache.log4j.Logger
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider
import org.json.{JSONException, JSONObject}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Created by venkatesh on 8/28/17.
  */
case class SolrCollectionStatus(machine: String, state: String, collectionName: String,
                                configName: String, coreName: String, shard: String)


object SolrClusterStatus {
  lazy val log = Logger.getLogger(SolrClusterStatus.getClass)

  var zkHostList: String = _
  // "listOfzookeepers"
  var chroot: String = _
  var collectionName: String = _
  var solrLiveNodes: List[String] = Nil
  var cloudClient: ZkClientClusterStateProvider = _

  // "zroot"
  def apply(zkList: String, zroot: String, collection: String): Unit = {
    zkHostList = zkList
    chroot = zroot
    collectionName = collection
    val zkHosts = zkHostList.split(",").toList.asJava
    cloudClient = new ZkClientClusterStateProvider(zkHosts, chroot)
    log.info(s"$zkHosts")
    log.info(s"$chroot")
    solrLiveNodes = getSolrNodes(cloudClient)
    log.info(s"${solrLiveNodes.toList}")
  }


  // solrMap("zkHosts")

  def getSolrNodes(cloudClient: ZkClientClusterStateProvider): List[String] = {
    cloudClient.connect()
    log.info(s"in getSolrNodes the value of cloud client:$cloudClient ")
    log.info(s"cloudclientObject $cloudClient")
    try {
      log.info(s"liveNodes ${cloudClient.liveNodes}")
    }
    catch {
      case e: Exception => {
        log.error("with live nodes", e)
        log.info(s"cloudclientObject $cloudClient")
        e.printStackTrace()
      }
    }
    val liveNodes = cloudClient.liveNodes()
      .asScala.toList
      .map(p => p.split("_")(0))
    require(liveNodes.length > 0, "there should be more than 1 live node the job to run")

    log.info(s"retrieved the solrnodes ${liveNodes.mkString(",")} from zookeeper")
    liveNodes
  }


  /**
    * This returns key,value pair of alias and the collection name
    *
    * @return
    */
  def getCollectionAliasMap(): Map[String, String] = {
    log.info("in collection alias map")
    val aliases: Map[String, String] = try {
      val clusterJsonResponse = new JSONObject(getClusterStatus(collectionName, false))
      val json = clusterJsonResponse.get("cluster").asInstanceOf[JSONObject]
        .get("aliases").asInstanceOf[JSONObject]
      val set = json.keys()
      val map = scala.collection.mutable.Map[String, String]()
      for (key <- set.asScala) {
        map(key) = json.getString(key)
      }
      map.toMap

    }
    catch {
      case e: Exception =>
        log.warn(s"Json was not proper", e)
        null
    }
    log.info(s"retrieved collection alais map $aliases")

    aliases
  }

  def getOldCollectionMapped(aliasName: String): String = {
    log.info(s"in getOldCollectionMapped alias name is $aliasName")
    val oldCollectionName = try {
      val oldAliases = getCollectionAliasMap()
      if (oldAliases != null) oldAliases.get(aliasName).get else null
    }
    catch {
      case e: JSONException => {
        log.warn(s"alias colection:$aliasName might not be present", e)
        null
      }
      case e1: Exception => {
        log.warn(s"alias colection:$aliasName might not be present", e1)
        null
      }
    }
    oldCollectionName
  }

  def getClusterStatus(collection: String, collectionNeeded: Boolean = true): String = {
    try {
      val node: String = solrLiveNodes(0)
      val collectionUrl: String = if (collectionNeeded) {
        s"&collection=$collection"
      } else {
        ""
      }
      val url = s"http://$node/solr/admin/collections?action=CLUSTERSTATUS&wt=json" + collectionUrl
      SolrOps.makeHttpRequest(url)
    }
    catch {
      case e: Exception => {
        log.error("could not retrieve cluster status and hence exiting", e)
        System.exit(-1)
        return null
      }
    }
  }

  def parseSolrResponse(): List[SolrCollectionStatus] = {
    val lb = new ListBuffer[SolrCollectionStatus]

    // Convert JSON string to JSONObject
    val solrResponseBody = getClusterStatus(this.collectionName)

    val clusterJsonResponse = new JSONObject(solrResponseBody)
    val test = clusterJsonResponse.get("cluster").asInstanceOf[JSONObject]
      .get("collections").asInstanceOf[JSONObject]
    val collectionName = test.keys().next().toString
    val configName = test.get(collectionName).asInstanceOf[JSONObject]
      .get("configName").toString

    val shardsNodes = test.get(collectionName).asInstanceOf[JSONObject]
      .get("shards").asInstanceOf[JSONObject]
    val shardKeys = shardsNodes.keys()

    while (shardKeys.hasNext()) {
      val shard = shardKeys.next().toString
      val coreNodes = shardsNodes.get(shard).asInstanceOf[JSONObject]
        .get("replicas").asInstanceOf[JSONObject]
      val coreKeys = coreNodes.keys()
      while (coreKeys.hasNext) {
        val coreKey = coreKeys.next().toString
        val coreNode = coreNodes.get(coreKey).asInstanceOf[JSONObject]
        val machine = coreNode.get("node_name").toString.split("_")(0)
        val coreName = coreNode.get("core").toString
        val state = coreNode.get("state").toString.toLowerCase

        lb.append(SolrCollectionStatus(machine, state, collectionName,
          configName, coreName, shard))

      }
    }
    lb.toList
  }
}
