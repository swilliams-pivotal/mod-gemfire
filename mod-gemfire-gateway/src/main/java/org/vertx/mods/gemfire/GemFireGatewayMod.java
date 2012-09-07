package org.vertx.mods.gemfire;

import java.io.IOException;
import java.util.List;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayEvent;
import com.gemstone.gemfire.cache.util.GatewayEventListener;
import com.gemstone.gemfire.cache.util.GatewayHub;

public class GemFireGatewayMod extends BusModBase implements Handler<Message<JsonObject>> {

  private Cache cache;

  private GatewayHub gatewayHub;

  @Override
  public void start() {
    super.start();

    JsonObject config = getContainer().getConfig();
    String address = config.getString("module-control-address", "vertx.gemfire.gateway.control");

    CacheConfigurer configurer = new CacheConfigurer(config);

    this.cache = configurer.create();

    JsonObject gatewayConfig = config.getObject("gateway-hub");

    String id = gatewayConfig.getString("hub-id");
    int port = gatewayConfig.getInteger("hub-port");

    this.gatewayHub = cache.addGatewayHub(id, port);

    String bindAddress = gatewayConfig.getString("bindAddress");
    if (bindAddress != null) {
      gatewayHub.setBindAddress(bindAddress);
    }

    boolean manualStart = gatewayConfig.getBoolean("manualStart", false);
    gatewayHub.setManualStart(manualStart);

    int maximumTimeBetweenPings = gatewayConfig.getInteger("maximumTimeBetweenPings");
    if (maximumTimeBetweenPings > -1) {
      gatewayHub.setMaximumTimeBetweenPings(maximumTimeBetweenPings);
    }

    int socketBufferSize = gatewayConfig.getInteger("socketBufferSize");
    if (socketBufferSize > -1) {
      gatewayHub.setSocketBufferSize(socketBufferSize);
    }

    String startupPolicy = gatewayConfig.getString("startupPolicy");
    if (startupPolicy != null) {
      gatewayHub.setStartupPolicy(startupPolicy);
    }

    JsonArray gateways = gatewayConfig.getArray("gateways");
    if (gateways != null) {
      for (Object o : gateways) {
        JsonObject gatewayConf = (JsonObject) o;
        registerGateway(gatewayConf);
      }
    }

    try {
      gatewayHub.start();

      eb.registerHandler(address, this);

    } catch (IOException e) {
      throw new GatewayInitializationException(e);
    }
  }

  @Override
  public void stop() throws Exception {
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
    if (event != null && event.body != null) {
      String type = event.body.getString("type");
      handleType(type, event);
    }
    else {
      event.reply();
    }
  }

  private void handleType(String type, Message<JsonObject> event) {
    if ("registerGateway".equals(type)) {
      JsonObject gateway = event.body.getObject("gateway");
      registerGateway(gateway);
    }
    else if ("unregisterGateway".equals(type)) {
      JsonObject gateway = event.body.getObject("gateway");
      unregisterGateway(gateway);
    }
  }


  private void registerGateway(JsonObject gatewayConf) {

    if (gatewayConf == null) {
      return;
    }

    final String gatewayId = gatewayConf.getString("id");
    final String address = gatewayConf.getString("address");
    int concurrency = gatewayConf.getInteger("concurrency");

    Gateway gateway = gatewayHub.addGateway(gatewayId, concurrency);
    gateway.addListener(new GatewayEventListener() {

      @Override
      public boolean processEvents(List<GatewayEvent> eventList) {

        int count = 0;
        for (GatewayEvent ge : eventList) {
          eb.send(address, ge.getSerializedValue());
          count++;
        }

        return count == eventList.size();
      }

      @Override
      public void close() { }
    });
  }

  private void unregisterGateway(JsonObject gateway) {
    if (gateway == null) {
      return;
    }

    final String gatewayId = gateway.getString("id");
    gatewayHub.removeGateway(gatewayId);
  }
}
