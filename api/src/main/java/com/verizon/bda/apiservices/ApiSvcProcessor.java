package com.verizon.bda.apiservices;

import java.util.Map;

/**
 * Created by chundch on 4/24/17.
 */

public interface ApiSvcProcessor {

   public String process(String resource, String resourceVersion,
                         Map<String, String> clientHeader, Object data);

}
