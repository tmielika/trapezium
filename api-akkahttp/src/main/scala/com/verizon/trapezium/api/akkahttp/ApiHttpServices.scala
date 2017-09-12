package com.verizon.trapezium.api.akkahttp

import com.verizon.trapezium.api.ApiServicesInterface

  /**
  * Created by chundch on 4/18/17.
  * Common Interface all http service apis
  */


trait ApiHttpServices extends ApiServicesInterface {

    /**
      * The common implementation for all http api services
      * processing.
      *
      * @param clientProfile
      * @param path
      * @param pathVersion
      * @param reqHeaderData
      * @param dataToProcess
      * @return processResult
      */

  private[api] final def processServiceRoute(clientProfile: String,
                          path: String,
                          pathVersion: String,
                          reqHeaderData: Map[String, String],
                          dataToProcess: Any): String = {


    import collection.JavaConverters._
     getProcessors.get(path).process(path, pathVersion, reqHeaderData.asJava,
        dataToProcess.asInstanceOf[ Object ])

  }



  }
