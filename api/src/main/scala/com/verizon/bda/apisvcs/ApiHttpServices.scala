package com.verizon.bda.apisvcs

import com.verizon.bda.apiservices.ApiServicesInterface

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

  private[apisvcs] final def processServiceRoute(clientProfile: String,
                          path: String,
                          pathVersion: String,
                          reqHeaderData: Map[String, String],
                          dataToProcess: Any): String = {


    import collection.JavaConverters._
     getProcessors.get(path).process(path, pathVersion, reqHeaderData.asJava,
        dataToProcess.asInstanceOf[ Object ])

  }



  }
