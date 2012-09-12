package org.vertx.mods.gemfire.support;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;

public class GatewayConfigurer {

  public static GatewayHub registerGatewayHub(Cache cache, JsonObject gatewayHubConfig) {

    String id = gatewayHubConfig.getString("hub-id");
    int port = gatewayHubConfig.getInteger("hub-port");

    GatewayHub gatewayHub = cache.addGatewayHub(id, port);

    String bindAddress = gatewayHubConfig.getString("bindAddress");
    if (bindAddress != null) {
      gatewayHub.setBindAddress(bindAddress);
    }

    boolean manualStart = gatewayHubConfig.getBoolean("manualStart", false);
    gatewayHub.setManualStart(manualStart);

    int maximumTimeBetweenPings = gatewayHubConfig.getInteger("maximumTimeBetweenPings");
    if (maximumTimeBetweenPings > -1) {
      gatewayHub.setMaximumTimeBetweenPings(maximumTimeBetweenPings);
    }

    int socketBufferSize = gatewayHubConfig.getInteger("socketBufferSize");
    if (socketBufferSize > -1) {
      gatewayHub.setSocketBufferSize(socketBufferSize);
    }

    String startupPolicy = gatewayHubConfig.getString("startupPolicy", "key");
    if (startupPolicy != null) {
      gatewayHub.setStartupPolicy(startupPolicy);
    }

    JsonArray gateways = gatewayHubConfig.getArray("gateways");
    if (gateways != null) {
      for (Object o : gateways) {
        JsonObject gatewayConf = (JsonObject) o;
        registerGateway(gatewayHub, gatewayConf);
      }
    }

    return gatewayHub;
  }

  public static Gateway registerGateway(GatewayHub gatewayHub, JsonObject gatewayConf) {

    if (gatewayConf == null) {
      return null;
    }

    String gatewayId = gatewayConf.getString("id");
    int concurrency = gatewayConf.getInteger("concurrency");
    Gateway gateway = gatewayHub.addGateway(gatewayId, concurrency);

//  final String address = gatewayConf.getString("address");

    String orderPolicyName = gatewayConf.getString("order-policy", "key");
    OrderPolicy orderPolicy = OrderPolicy.valueOf(orderPolicyName);
    gateway.setOrderPolicy(orderPolicy);

    GatewayQueueAttributes queueAttributes = new GatewayQueueAttributes();
    JsonObject queueAttributeConf = gatewayConf.getObject("queue-attributes");

    int threshold = queueAttributeConf.getInteger("threshold");
    queueAttributes.setAlertThreshold(threshold);

    boolean batchConflation = queueAttributeConf.getBoolean("batch-conflation");
    queueAttributes.setBatchConflation(batchConflation);

    int batchSize = queueAttributeConf.getInteger("batch-size");
    queueAttributes.setBatchSize(batchSize);

    int batchTimeInterval = queueAttributeConf.getInteger("batch-time-interval");
    queueAttributes.setBatchTimeInterval(batchTimeInterval);

    String diskStoreName = queueAttributeConf.getString("disk-store-name");
    queueAttributes.setDiskStoreName(diskStoreName);

    boolean enablePersistence = queueAttributeConf.getBoolean("enable-persistence");
    queueAttributes.setEnablePersistence(enablePersistence);

    int maximumQueueMemory = queueAttributeConf.getInteger("");
    queueAttributes.setMaximumQueueMemory(maximumQueueMemory);
    gateway.setQueueAttributes(queueAttributes);

    int socketBufferSize = gatewayConf.getInteger("socket-buffer-size");
    gateway.setSocketBufferSize(socketBufferSize);

    int socketReadTimeout = gatewayConf.getInteger("socket-read-timeout");
    gateway.setSocketReadTimeout(socketReadTimeout);

    JsonArray endpoints = gatewayConf.getArray("endpoints");
    for (Object o : endpoints) {

      JsonObject endpoint = (JsonObject) o;
      String id = endpoint.getString("id");
      String addr = endpoint.getString("address");
      int port = endpoint.getInteger("port");

      gateway.addEndpoint(id, addr, port);
    }

    return gateway;

  }


}
