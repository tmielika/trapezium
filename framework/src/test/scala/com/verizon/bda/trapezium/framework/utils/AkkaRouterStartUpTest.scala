/**
* Copyright (C) 2016 Verizon. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.verizon.bda.trapezium.framework.utils

import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.server.utils.AkkaRouterStartUp
import org.apache.spark.streaming.TestSuiteBase
import com.verizon.bda.trapezium.cache.CacheConfig
/**
  * Created by v468328 on 11/23/16.
  */
class AkkaRouterStartUpTest extends TestSuiteBase {


  /* test("cache conf load  test") {


    AkkaRouterStartUp.loadConfig()

    assert(CacheConfig.config.getString("commons.cache.impl").
      equals("com.verizon.bda.trapezium.store.cache.mock.MockCacheCBImpl"))

    // now assert some properties


    assert(CacheConfig.config.getString("cbc.url").
      equals("http://localhost:60591/pools"))


  } */

}
