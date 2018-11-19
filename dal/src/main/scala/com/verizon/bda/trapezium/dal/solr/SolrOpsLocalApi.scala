package com.verizon.bda.trapezium.dal.solr

import com.verizon.bda.trapezium.dal.exceptions.SolrOpsException
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

class SolrOpsLocalApi(solrMap: Map[String, String], sparkContext: SparkContext)
  extends SolrOpsLocal(solrMap: Map[String, String]) {

  override def getHostToFileMap(): Map[String, ListBuffer[(String, String)]] = {
    log.info("inside SolrOpsLocal.getHostToFileMap")
    try {
      val isRunning = PostZipDataAPI.isApiRunningOnAllMachines(coreMap, solrMap)
      if (isRunning) {
        return PostZipDataAPI.postDataViaHTTP(sparkContext, solrMap, hdfsIndexFilePath,
          coreMap, collectionName)
      } else {
        throw new SolrOpsException(s"could not create collection :$collectionName")
      }
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
      PostZipDataAPI.deleteDirectoryViaHTTP(oldCollection, coreMap, solrMap)
    }
  }


}
