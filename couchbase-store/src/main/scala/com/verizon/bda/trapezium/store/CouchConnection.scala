package com.verizon.bda.trapezium.store

import com.couchbase.client.CouchbaseClient

/**
  * Created by v468328 on 9/23/16.
  * This connection work with CouchBase and very specfic to CouchBase datatypes.
  * We are java driver as it is mature and battle tested as compared to
  * scala driver which is still work in progress.
  */
class CouchConnection(mclient: CouchbaseClient) {

  val expirationTimeInSeconds: Int = 60 * 60 * 24

  def get(key: String): Object = {
    val result = mclient.get(key);

    result
  }

  def put(key: String, value: Object, ttl: Int) {
    mclient.set(key, ttl, value);

  }

  def put(keyMap: scala.collection.Map[String, Object], ttl: Int) {

    val walker = keyMap.keysIterator
    while (walker.hasNext) {
      val key = walker.next()
      mclient.set(key, ttl, keyMap.get(key))
    }

  }

  def delete(key: String): Boolean = {
    mclient.delete(key);
    true;
  }

  def flush() : Unit = {
    mclient.flush()

  }



}
