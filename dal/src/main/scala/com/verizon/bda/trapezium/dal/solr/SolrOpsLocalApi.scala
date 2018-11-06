package com.verizon.bda.trapezium.dal.solr

import com.verizon.bda.trapezium.dal.util.zookeeper.ZooKeeperClient
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

class SolrOpsLocalApi(solrMap: Map[String, String], sparkContext: SparkContext)
  extends SolrOpsLocal(solrMap: Map[String, String]) {

  override def getHostToFileMap(): Map[String, ListBuffer[(String, String)]] = {
    try {
      return PostZipDataAPI.postDataViaHTTP(sparkContext, solrMap, hdfsIndexFilePath,
        indexLocationInRoot, coreMap, collectionName)
    }
    catch {
      case e: Exception => {
        rollBackCollections(collectionName)
        log.error(s"could not create collection ${collectionName}", e)
        return null
      }
    }
  }

  override def deleteOldCollections(oldCollection: String): Unit = {
    if (oldCollection != null) {
      deleteCollection(oldCollection, false)
      PostZipDataAPI.deleteDirectoryViaHTTP(oldCollection, map.keySet, solrMap)
    }
  }


}
