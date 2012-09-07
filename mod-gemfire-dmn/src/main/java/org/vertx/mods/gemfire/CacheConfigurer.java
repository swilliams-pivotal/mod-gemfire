package org.vertx.mods.gemfire;

import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;

public class CacheConfigurer {

  private JsonObject config;

  public CacheConfigurer(JsonObject config) {
    this.config = config;
  }

  public Cache create() {
    CacheFactory factory = new CacheFactory();

    return factory.create();
  }

}
