package com.verizon.bda.apisvcs.security

import com.verizon.bda.apisvcs.utils.HttpServicesUtils._
import com.verizon.bda.apisvcs.utils.HttpServicesConstants._
import com.verizon.bda.commons.serviceapis.security.BDAAuthSvcManager
import org.slf4j.LoggerFactory

import scala.util.Try


/**
  * Created by chundch on 5/1/17.
  */

class ApiAuthorizationService {

   private val logger = LoggerFactory.getLogger(this.getClass)

   private var authSvcProviderType : String = _

   def getAuthSvcProivderType(): String = {
      authSvcProviderType
   }

   def setAuthSvcProivderType (provider : String): Unit = {

      if (provider == null || provider.isEmpty) {

         logger.info(s"auth provider is set to $WSO2_AUTHORIZATION_PROVIDER")
         authSvcProviderType = WSO2_AUTHORIZATION_PROVIDER
      } else {
         logger.info(s"auth provider is set to $provider")
         authSvcProviderType = provider
      }
   }


   def authorizeApiClient(authData: Map[String, String] ) : (Boolean, Any) = {

      logger.info("authorization data map size : " + (if (authData != null) authData.size else 0  ))
      var validClient : Boolean = false
      var authdata : Any = null
      val apiSvcAuth = BDAAuthSvcManager.getApiSvcsAuthorizer(authSvcProviderType)
      val keyslist = authorizationDataAccessKeys()
      val clientdata = getAuthorizationElementsFromAuthData(authData, keyslist)

      if(clientdata != null && clientdata.size == 1){

         val authdatakey = keyslist(0)
         val authheaderdata = clientdata.get(authdatakey).get
         authdata = apiSvcAuth.getClientAuthorizationProfile(
            authheaderdata)
         if(authdata != null) validClient = true

      }

      (validClient, authdata)

   }



   def authorizationDataAccessKeys(): List[String] = {

      var keysList : List[String] = null

      authSvcProviderType match {
         case WSO2_AUTHORIZATION_PROVIDER => {
            keysList = List(WSO2_AUTHORIZATION_DATA_KEY)
         }

         case COUCHBASE_AUTHORIZATION_PROVIDER => {

            keysList = List(VZ_DATE_HEADER_KEY, VZ_AUTHORIZATION_HEADER_KEY)
         }

         case _ => {
            throw new Exception("un supported authorization provider")
         }
      }


      logger.info("authorization data access keys size : " + keysList.size)

      keysList

   }


}
