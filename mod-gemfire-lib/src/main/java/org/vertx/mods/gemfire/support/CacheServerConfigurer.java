package org.vertx.mods.gemfire.support;

import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ServerLoadProbe;

public class CacheServerConfigurer {

  public static CacheServer configure(Cache cache, JsonObject config) {
    CacheServer server = cache.addCacheServer();

    String bindAddress = config.getString("bind-address", "0.0.0.0");
    server.setBindAddress(bindAddress);

    String[] groups = config.getString("groups", "").split(",");
    if (groups != null && groups.length > 0) {
      server.setGroups(groups);
    }

    String hostnameForClients = config.getString("hostname-for-clients");
    if (hostnameForClients != null) {
      server.setHostnameForClients(hostnameForClients);
    }

    long loadPollInterval = config.getLong("load-poll-interval");
    server.setLoadPollInterval(loadPollInterval);

    String loadProbeClassName = config.getString("load-probe-class");
    if (loadProbeClassName != null) {
      ServerLoadProbe loadProbe = InstantiationUtils.instantiate(ServerLoadProbe.class, loadProbeClassName);
      server.setLoadProbe(loadProbe);
    }

    int maxConnections = config.getInteger("max-connections");
    if (maxConnections > -1) {
      server.setMaxConnections(maxConnections);
    }

    int maximumMessageCount = config.getInteger("max-message-count");
    server.setMaximumMessageCount(maximumMessageCount);

    int maximumTimeBetweenPings = config.getInteger("max-time-between-pings");
    server.setMaximumTimeBetweenPings(maximumTimeBetweenPings);

    int maxThreads = config.getInteger("max-threads");
    server.setMaxThreads(maxThreads);

    int messageTimeToLive = config.getInteger("message-ttl");
    server.setMessageTimeToLive(messageTimeToLive);

    int port = config.getInteger("port");
    server.setPort(port);

    int socketBufferSize = config.getInteger("socket-buffer-size");
    server.setSocketBufferSize(socketBufferSize);

    return server;
  }

}
