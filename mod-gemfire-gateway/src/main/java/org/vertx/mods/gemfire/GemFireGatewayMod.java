package org.vertx.mods.gemfire;

import java.io.IOException;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.mods.gemfire.support.CacheConfigurer;
import org.vertx.mods.gemfire.support.EventBusCacheListener;
import org.vertx.mods.gemfire.support.GatewayConfigurer;
import org.vertx.mods.gemfire.support.RegionConfigurer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;

public class GemFireGatewayMod extends BusModBase implements Handler<Message<JsonObject>> {

  private Cache cache;

  private GatewayHub gatewayHub;

  private String controlHandler;

  @Override
  public void start() {
    super.start();

    try {
      JsonObject config = getContainer().getConfig();
      String address = config.getString("module-control-address", "vertx.gemfire.gateway.control");

      this.cache = CacheConfigurer.configure(config);

      JsonArray regions = config.getArray("regions");
      registerRegions(regions);

      JsonObject gatewayHubConfig = config.getObject("gateway-hub");
      this.gatewayHub = GatewayConfigurer.registerGatewayHub(cache, gatewayHubConfig);

      gatewayHub.start();

      this.controlHandler = eb.registerHandler(address, this);

    } catch (IOException | ClassNotFoundException e) {
      throw new GatewayInitializationException(e);
    }
  }

  @Override
  public void stop() throws Exception {

    if (controlHandler != null) {
      eb.unregisterHandler(controlHandler);
    }

    if (gatewayHub != null) {
      gatewayHub.stop();
    }

    if (cache != null) {
      cache.close();
    }

    super.stop();
  }

  @Override
  public void handle(Message<JsonObject> event) {
    String type = super.getMandatoryString("type", event);

    if (event != null && event.body != null) {
      handleType(type, event);
    }
    else {
      event.reply();
    }
  }

  private void handleType(String type, Message<JsonObject> event) {
    if ("registerGateway".equals(type)) {
      JsonObject gatewayConfig = event.body.getObject("gateway");
      String address = gatewayConfig.getString("address");

      Gateway gateway = GatewayConfigurer.registerGateway(gatewayHub, gatewayConfig);
      gateway.addListener(new EventBusGatewayEventListener(eb, address));
    }
    else if ("unregisterGateway".equals(type)) {
      JsonObject gateway = event.body.getObject("gateway");
      unregisterGateway(gateway);
    }
  }

  private void registerRegions(JsonArray regions) throws ClassNotFoundException {
    for (Object o : regions) {
      JsonObject regionConfig = (JsonObject) o;
      Region<Object, Object> region = RegionConfigurer.registerRegion(cache, eb, regionConfig);

      String regionAddress = regionConfig.getString("address");

      CacheListener<Object, Object> cacheListener = new EventBusCacheListener(eb, regionAddress);
      region.getAttributesMutator().addCacheListener(cacheListener);
    }
  }

  private void unregisterGateway(JsonObject gateway) {
    if (gateway == null) {
      return;
    }

    final String gatewayId = gateway.getString("id");
    gatewayHub.removeGateway(gatewayId);

    // TODO remove handler
  }
}
