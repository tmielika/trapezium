package com.verizon.bda.trapezium.dal.util.zookeper
import com.verizon.bda.trapezium.dal.util.zookeeper.ZooKeeperClient
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class ZookeeperTest  extends FlatSpec with Matchers with BeforeAndAfter{
  var zk: EmbeddedZookeeper = null
   before{
    zk = new EmbeddedZookeeper("127.0.0.1:2181")
    ZooKeeperClient("127.0.0.1:2181")
  }
  "ZookeperClient" should "be able insert and access data" in {
    ZooKeeperClient.setData("/test11/test111", "test data".getBytes)
//    println(ZooKeeperClient.getData("/test11/test111"))
    ZooKeeperClient.getData("/test11/test111") should be ("test data")
  }
   after{
    zk.shutdown()
   }

}

