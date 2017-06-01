package com.verizon.bda.trapezium.framework

import org.json.JSONArray
import org.json.JSONObject

/**
  * Created by v708178 on 6/1/17.
  */

class TriggerStruct (val dataSource: String, val arr: Array[DataSourceNameLocation]){

  override def toString () : String = {


    val jArr = new JSONArray()
    arr.foreach( dataSource => {
      val json = new JSONObject()
      json.put("name", dataSource.sourceName)
      json.put("location" , dataSource.path)
      jArr.put(json)
    })
    val jsonmain = new JSONObject()
    jsonmain.put("datasources", jArr )
    jsonmain.toString()
  }
}

class DataSourceNameLocation(val sourceName : String, val path : String){

}

