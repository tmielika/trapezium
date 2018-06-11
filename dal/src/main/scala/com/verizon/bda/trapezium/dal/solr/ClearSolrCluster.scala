package com.verizon.bda.trapezium.dal.solr

import com.typesafe.config.Config
import org.apache.log4j.Logger
import scopt.OptionParser
import com.typesafe.config.{Config, ConfigFactory}
import java.io.File
import scala.collection.mutable.ListBuffer

object ClearSolrCluster {

  case class ClearSolrNodes(configDir: String = null,
                            configFile: String = null,
                            collections: String = null)

  lazy val log = Logger.getLogger(classOf[ClearSolrNodes])

  def main(args: Array[String]): Unit = {
    val parser = inputParser
    val clearSolrNodes = parser.parse(args, ClearSolrNodes()).get
    val configDir: String = clearSolrNodes.configDir
    val configFile: String = clearSolrNodes.configFile
    val collections: Array[String] = clearSolrNodes.collections.split(",")
    val config: Config = readConfigs(configDir, configFile)
    val zkList = config.getString("solr.zkhosts")
    val zroot = config.getString("solr.zroot")
    val solrNodeUser = config.getString("solr.node_ssh_user")
    val storageDir = config.getString("solr.storageDir")
    //    val machinePrivateKey = config.getString("machinePrivateKey")
    val rootDirs = config.getString("solr.disks")
    val folderPrefix = config.getString("solr.index_folder_prefix")
    val solrMap = Map(
      "solrUser" -> solrNodeUser,
      "rootDirs" -> rootDirs,
      "folderPrefix" -> folderPrefix,
      "storageDir" -> "/solrtest/",
      "appName" -> storageDir,
      "numHTTPTasks" -> "20"
    )
    SolrClusterStatus(zkList, zroot, "")
    val hosts = SolrClusterStatus.solrLiveNodes.map(p => p.split(":")(0)).toList

    CollectIndices.getMachineMap(hosts, solrNodeUser, null)
    collections.foreach(collection => new SolrOpsLocal(solrMap).deleteOldCollections(collection))

  }

  def inputParser(): OptionParser[ClearSolrNodes] = {
    new OptionParser[ClearSolrNodes]("ClearSolrCluster") {
      head("ClearSolrCluster service for clearing the cluster ")
      opt[String]("config")
        .text(s"local config directory path")
        .optional
        .action((x, c) => c.copy(configDir = x))
      opt[String]("file")
        .text(s"config file path where all the configurations ")
        .required
        .action((x, c) => c.copy(configFile = x))
      opt[String]("collections")
        .text(s"collections to delete")
        .required
        .action((x, c) => c.copy(collections = x))
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
