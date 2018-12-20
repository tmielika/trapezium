package com.verizon.bda.trapezium.framework.zookeeper

import java.io.File
import java.net.InetSocketAddress

import org.apache.commons.io
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

import scala.util.Random

/**
 * Created by parmana on 12/20/18.
 */
class EmbeddedZookeeper (val zkConnect: String) extends {
  val random = new Random()
  val snapshotDir = new File("target/snapshotDir")
  val logDir = new File("target/zk_logDir")

  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)

  val splits = zkConnect.split(":")
  val ip = splits(0)
  var port = splits(1).toInt

  val factory = new NIOServerCnxnFactory()

  // for local as well as jenkins build

  factory.configure(new InetSocketAddress(ip, port), 16)
  factory.startup(zookeeper)

  val actualPort = factory.getLocalPort

  def shutdown() {
    factory.shutdown()
    deleteRecursively(snapshotDir)
    deleteRecursively(logDir)
  }

  private def deleteRecursively(in: File): Unit = {

    if (in.isDirectory) {
      io.FileUtils.deleteDirectory(in)
    } else {
      in.delete()
    }
  }
}