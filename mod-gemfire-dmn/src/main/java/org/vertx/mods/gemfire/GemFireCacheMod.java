package org.vertx.mods.gemfire;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.server.CacheServer;

public class GemFireCacheMod extends BusModBase implements Handler<Message<JsonObject>> {

  private Cache cache;

  @Override
  public void start() {
    super.start();

    JsonObject config = getContainer().getConfig();

    CacheConfigurer configurer = new CacheConfigurer(config);

    this.cache = configurer.create();

    JsonObject cacheConfig = config.getObject("cache");
    JsonArray cacheServers = cacheConfig.getArray("servers");
    for (Object o : cacheServers) {
      JsonObject cacheServer = (JsonObject) o;
      addCacheServer(cacheServer);
    }
  }

  @Override
  public void stop() throws Exception {

    super.stop();
  }

  @Override
  public void handle(Message<JsonObject> event) {
    // TODO Auto-generated method stub
    
  }

  private void addCacheServer(JsonObject cacheServerConfig) {
    CacheServer cacheServer = cache.addCacheServer();

    String bindAddress = cacheServerConfig.getString("bindAddress");
    cacheServer.setBindAddress(bindAddress);
  }

}
