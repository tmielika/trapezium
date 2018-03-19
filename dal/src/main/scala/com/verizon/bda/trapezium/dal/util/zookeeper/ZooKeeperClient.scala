package com.verizon.bda.analytics.api.dao.zk

import org.apache.commons.io.Charsets
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.CloseableUtils

object ZooKeeperClient {
  var curatorFramework: CuratorFramework = _

  def apply(quorum: String, retryCount: Int = 3, connectionTimeoutInMillis: Int = 2 * 1000,
            sessionTimeoutInMillis: Int = 60 * 1000): Unit = {
    val retryPolicy = new ExponentialBackoffRetry(1000, 10)
    curatorFramework = init(quorum, connectionTimeoutInMillis, sessionTimeoutInMillis, retryPolicy)
  }

  def create(znode: String): Unit = {
    curatorFramework.create().forPath(znode)
  }

  def setData(znode: String, data: Array[Byte]): Unit = {
    if (hasZnodePath(znode)) {
      curatorFramework.setData().forPath(znode, data)
    } else {
      curatorFramework.create.creatingParentsIfNeeded.forPath(znode, data)
    }
    // curatorFramework.setData().forPath(znode, data)
  }

  def getData(znode: String): String = {
    require(znode != null && znode.nonEmpty, "Key cannot be null or empty")
    //    val dat = curatorFramework.getData().forPath(znode)
    //    new String(dat, "UTF-8")
    new String(curatorFramework.getData().forPath(znode))
  }


  def delete(znode: String): Unit = {
    curatorFramework.delete().deletingChildrenIfNeeded().forPath(znode)
  }

  def hasZnodePath(znode: String): Boolean = {
    curatorFramework.checkExists().forPath(znode) != null
  }

  def close(): Unit = {
    CloseableUtils.closeQuietly(curatorFramework)
  }

  import org.apache.curator.framework.CuratorFramework
  import org.apache.zookeeper.CreateMode

  @throws[Exception]
  def createEphemeral(client: CuratorFramework, path: String, payload: Array[Byte]): Unit = {
    // this will create the given EPHEMERAL ZNode with the given data
    client.create.creatingParentsIfNeeded.withMode(CreateMode.EPHEMERAL).forPath(path, payload)
  }


  def init(zkQuorum: String, connectionTimeoutInMillis: Int,
           sessionTimeoutInMillis: Int, retryPolicy: RetryPolicy):
  CuratorFramework = {
    val client = CuratorFrameworkFactory.builder()
      .connectString(zkQuorum)
      .retryPolicy(retryPolicy)
      .connectionTimeoutMs(connectionTimeoutInMillis)
      .sessionTimeoutMs(sessionTimeoutInMillis)
      .build()
    client.start()
    client
  }

  def main(args: Array[String]): Unit = {
    ZooKeeperClient("istgbd019.verizon.com:2181," +
      "istgbd018.verizon.com:2181,istgbd017.verizon.com:2181")
    ZooKeeperClient.setData("/test11/test111", "hello world1".getBytes)
    //    println(ZooKeeperClient.delete("/test11"))
    //    println()
  }
}