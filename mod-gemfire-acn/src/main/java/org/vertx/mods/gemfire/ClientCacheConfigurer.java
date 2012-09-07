/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.vertx.mods.gemfire;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;

public class ClientCacheConfigurer {

  private JsonObject config;

  public ClientCacheConfigurer(JsonObject config) {
    this.config = config;
  }

  public ClientCache create() {
    ClientCacheFactory factory = new ClientCacheFactory();

    configureProperties(factory);

    configureLocators(factory);

    configurePool(factory);

    return factory.create();
  }

  private void configureProperties(ClientCacheFactory factory) {
    JsonObject properties = config.getObject("properties");

    if (properties == null) {
      return;
    }

    if (properties != null) {
      for (String name : properties.getFieldNames()) {
        String value = properties.getString(name);
        factory.set(name, value);
      }
    }
  }

  private void configureLocators(ClientCacheFactory factory) {
    JsonArray poolLocators = config.getArray("poolLocators");

    if (poolLocators == null) {
      return;
    }

    for (Object o : poolLocators) {
      JsonObject locator = (JsonObject) o;
      String host = locator.getString("host");
      Number port = locator.getNumber("port");
      factory.addPoolLocator(host, port.intValue());
    }
  }

  private void configurePool(ClientCacheFactory factory) {
    JsonArray poolServers = config.getArray("poolServers");

    if (poolServers == null) {
      return;
    }

    for (Object o : poolServers) {
      JsonObject locator = (JsonObject) o;
      String host = locator.getString("host");
      Number port = locator.getNumber("port");
      factory.addPoolServer(host, port.intValue());
    }
  }
}
