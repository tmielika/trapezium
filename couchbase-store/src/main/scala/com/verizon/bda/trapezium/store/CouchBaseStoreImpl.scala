package com.verizon.bda.trapezium.store

import java.net.URI
import java.util
import com.couchbase.client.CouchbaseClient
import com.typesafe.config.ConfigFactory
import com.verizon.bda.trapezium.cache.CacheConfig
import com.verizon.bda.trapezium.cache.exception.CacheException
import com.verizon.bda.trapezium.dal.data.KVStore
import com.verizon.bda.trapezium.cache
import org.slf4j.LoggerFactory

/**
  * Created by v468328 on 9/22/16.
  * The default implementation for cache (CouchBase) provided by framework.
  * The bucket in couch base is like schema.
  * App specfic buckets needs user/password
  */

@SerialVersionUID(228L)
class CouchBaseStoreImpl[K, V]() extends
KVStore[K, V] {
  val logger = LoggerFactory.getLogger(this.getClass)
  private var couchBaseUrl = CacheConfig.config.getString("cbc.url")

  protected var couchConnection: CouchConnection = null

  protected val keyNotNull =
    CacheConfig.config.getString("commons.cache.keyNotFound")
  protected val wrongParams =
    CacheConfig.config.getString("commons.cache.wrongParams")

  @throws[Exception]
  def init(params: String*): Unit = {

    logger.debug("Initializing logger cache")

    var couchBaseClient: CouchbaseClient = null

    if (params.length == 2) {
      // it is case with specfic bucket and password
      val url: URI = new URI(couchBaseUrl)
      val baseList: java.util.List[java.net.URI] = new util.ArrayList[java.net.URI]
      baseList.add(url)
      logger.debug("params for cache are param 1 > " + params.apply(0)
        + " param 2 > " + params.apply(1) + " couchBaseUrl > " + couchBaseUrl)
      couchBaseClient = new CouchbaseClient(baseList, params.apply(0), params.apply(1))
      couchConnection = new CouchConnection(couchBaseClient)

    }
    else {
      throw new CacheException(wrongParams)
    }

  }

  override
  def put(key: K, value: V): Unit = {

    if (key == null) throw new CacheException(keyNotNull)
    logger.debug("putting key in cache key " + key.hashCode() + " value not null = " +
      (value == null))
    couchConnection.put(key.hashCode() + "", value.asInstanceOf[AnyRef], 24 * 60 * 60)
  }


  override
  def put(key: K, value: V, ttl: Int = 24 * 60 * 60): Unit = {

    if (key == null) throw new CacheException(keyNotNull)
    logger.debug("putting key in cache key " + key.hashCode() + " value = " + value)
    couchConnection.put(key.hashCode() + "", value.asInstanceOf[AnyRef], ttl)
  }

  def get(key: K): Option[V] = {

    if (key == null) throw new CacheException(keyNotNull)
    val result = couchConnection.get(key.hashCode() + "").asInstanceOf[V]
    // capture statistics of how many get are sucess for hit ratio
    logger.info("result of get is for key " + key + " value= " + result)
    Option(result)

  }

  def putMulti(keyMap: Map[K, V], ttl: Int = 24 * 60 * 60): Unit = {

    couchConnection.put(transform(keyMap), ttl)
  }

  def getMulti(keyList: Seq[K]): Map[K, V] = {

    var transformedMap: scala.collection.immutable.Map[K, V] =
      scala.collection.immutable.Map[K, V]()

    keyList.foreach(k => {
      if (k == null) throw new CacheException(keyNotNull)
      val value: Object = couchConnection.get(k.hashCode() + "")
      logger.info("result of get is for key " + k + " value= " + value)
      transformedMap += (k -> (value.asInstanceOf[V]))

    })

    transformedMap
  }


  def delete(key: K): Unit = {
    if (key == null) throw new CacheException(keyNotNull)
    logger.debug("Trying to delete key " + key.hashCode())
    couchConnection.delete(key.hashCode() + "")

  }

  private def transform(keyMap: Map[K, V]): Map[String, Object] = {

    var transformedMap: scala.collection.immutable.Map[String, Object] =
      scala.collection.immutable.Map[String, AnyRef]()

    for ((k, v) <- keyMap) {
      if (k == null) throw new CacheException(keyNotNull)
      transformedMap += (k.hashCode() + "" -> v.asInstanceOf[Object])
    }
    transformedMap
  }


  private def transform(keyList: Seq[K]): Seq[String] = {

    val transformedList = new scala.collection.mutable.ListBuffer()

    for (k <- keyList) {
      if (k == null) throw new CacheException(keyNotNull)
      transformedList +: (k.hashCode() + "")

    }

    transformedList.asInstanceOf[Seq[String]]

  }

  def flush(): Unit = {

    logger.debug("master flush called by client ")
    couchConnection.flush()
  }


}
