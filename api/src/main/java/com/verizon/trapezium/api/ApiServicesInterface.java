package com.verizon.trapezium.api;


import java.util.HashMap;

/**
 *  Api Services Interface
 */

public interface ApiServicesInterface {

  public HashMap<String, ApiSvcProcessor> getProcessors();

  public String getApiSvcAuthorizer();

 }