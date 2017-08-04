package com.verizon.bda.trapezium.framework.handler
import com.verizon.bda.trapezium.framework.{ApplicationManagerTestSuite}
import scala.collection.mutable.{Map => MMap}
import org.json._
/**
  * Created by v708178 on 5/31/17.
  */
class WorkflowTrigger extends ApplicationManagerTestSuite{
  test("Test Json Parsing") {
    var dataMap = MMap[String, String]()
    val json = "{ \"datasources\" : [ { \"name\": \"test\",  \"location\": \"testlocation\"}]}"
    val jObject = new JSONObject(json)
    val jArray = jObject.getJSONArray("datasources")
    for( i <- 0 to jArray.length()-1){
      logger.info(s"input source is Parquet" )
      val jObject = jArray.get(i).asInstanceOf[JSONObject]
      val sourcesName = jObject.getString("name")
      val sourceLocation = jObject.getString("location")
      dataMap += ((sourcesName, sourceLocation ))
    }
    assert(dataMap.size==1)
  }


  test("Test JSON DF"){
    val json = "{ \"datasources\" : [ { \"name\": \"test\",  \"location\":" +
      " \"src/test/data/parquet\"}]}"
    val df = FileSourceGenerator.getDFFromStream(json, sc)
    val df1 = df("test")
    assert(df.size==1)
    assert(df1.count()>1)
  }

}
