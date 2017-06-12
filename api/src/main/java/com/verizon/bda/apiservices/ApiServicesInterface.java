package com.verizon.bda.apiservices;


import java.util.HashMap;

/**
 *  Api Services Interface
 */

public interface ApiServicesInterface {

  public HashMap<String, ApiSvcProcessor> getProcessors();

  public String getApiSvcAuthorizer();

 }