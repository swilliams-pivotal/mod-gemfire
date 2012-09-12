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
package org.vertx.mods.gemfire.support;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.vertx.java.core.file.FileSystem;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.pdx.PdxSerializer;

public class ClientCacheConfigurer {

  public static ClientCache configure(FileSystem fs, JsonObject config) {

    ClientCacheFactory factory = new ClientCacheFactory();

    configurePropertiesFile(factory, config);

    configureProperties(factory, config);

    configurePoolProperties(factory, config);

    configureLocators(factory, config);

    configurePool(factory, config);

    configurePDX(factory, config);

    ClientCache cache =  factory.create();

    // FIXME this all seems a bit risky
    String cacheXmlFile = config.getString("cache-xml-file", "client-cache.xml");
    System.out.printf("cacheXmlFile: %s%n", cacheXmlFile);

    try (InputStream is = new FileInputStream(new File(cacheXmlFile))) {
      cache.loadCacheXml(is);
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return cache;
  }

  private static void configurePropertiesFile(ClientCacheFactory factory, JsonObject config) {

    String propertiesFile = config.getString("properties-file", "gemfire.properties");
    System.out.printf("propertiesFile: %s%n", propertiesFile);

    if (propertiesFile == null) {
      return;
    }

    Properties properties = new Properties();

    try (InputStream is = new FileInputStream(new File(propertiesFile))) {
      if (propertiesFile.endsWith(".xml")) {
        properties.loadFromXML(is);
      }
      else {
        properties.load(is);
      }

      for (String name : properties.stringPropertyNames()) {
        String value = properties.getProperty(name);
        factory.set(name, value);
      }

    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }


  private static void configureProperties(ClientCacheFactory factory, JsonObject config) {

    JsonObject propertiesConf = config.getObject("properties");

    if (propertiesConf == null || propertiesConf.getFieldNames().size() == 0) {
      return;
    }


    if (propertiesConf != null) {
      for (String name : propertiesConf.getFieldNames()) {
        String value = propertiesConf.getString(name);
        factory.set(name, value);
      }
    }
  }

  private static void configurePoolProperties(ClientCacheFactory factory, JsonObject config) {
    JsonObject properties = config.getObject("pool-properties");

    if (properties == null || properties.getFieldNames().size() == 0) {
      return;
    }

    int connectionTimeout = properties.getInteger("connection-timeout");
    factory.setPoolFreeConnectionTimeout(connectionTimeout);

    long idleTimeout = properties.getLong("idle-timeout");
    factory.setPoolIdleTimeout(idleTimeout);

    int loadConditioningInterval = properties.getInteger("load-conditioning-interval");
    factory.setPoolLoadConditioningInterval(loadConditioningInterval);

    int maxConnections = properties.getInteger("max-connections");
    factory.setPoolMaxConnections(maxConnections);

    int minConnections = properties.getInteger("min-connections");
    factory.setPoolMinConnections(minConnections);

    boolean multiuserAuthenticationEnabled = properties.getBoolean("multiuser-authentication");
    factory.setPoolMultiuserAuthentication(multiuserAuthenticationEnabled);

    long pingInterval = properties.getLong("ping-interval");
    factory.setPoolPingInterval(pingInterval);

    boolean pRSingleHopEnabled = properties.getBoolean("pr-single-hop");
    factory.setPoolPRSingleHopEnabled(pRSingleHopEnabled);

    int poolReadTimeout = properties.getInteger("pool-read-timeout");
    factory.setPoolReadTimeout(poolReadTimeout);

    int retryAttempts = properties.getInteger("retry-attempts");
    factory.setPoolRetryAttempts(retryAttempts);

    String group = properties.getString("group");
    factory.setPoolServerGroup(group);

    int bufferSize = properties.getInteger("buffer-size");
    factory.setPoolSocketBufferSize(bufferSize);

    int statisticInterval = properties.getInteger("statistic-interval");
    factory.setPoolStatisticInterval(statisticInterval);

    int ackInterval = properties.getInteger("ack-interval");
    factory.setPoolSubscriptionAckInterval(ackInterval);

    boolean subscriptionEnabled = properties.getBoolean("subscription-enabled");
    factory.setPoolSubscriptionEnabled(subscriptionEnabled);

    int messageTrackingTimeout = properties.getInteger("message-tracking-timeout");
    factory.setPoolSubscriptionMessageTrackingTimeout(messageTrackingTimeout);

    int redundancy = properties.getInteger("redundancy");
    factory.setPoolSubscriptionRedundancy(redundancy);

    boolean threadLocalConnections = properties.getBoolean("thread-local-connections");
    factory.setPoolThreadLocalConnections(threadLocalConnections);

  }

  private static void configureLocators(ClientCacheFactory factory, JsonObject config) {
    JsonArray poolLocators = config.getArray("pool-locators");

    if (poolLocators == null) {
      return;
    }

    for (Object o : poolLocators) {
      JsonObject locator = (JsonObject) o;
      String host = locator.getString("host");
      int port = locator.getInteger("port");
      factory.addPoolLocator(host, port);
    }
  }

  private static void configurePool(ClientCacheFactory factory, JsonObject config) {
    JsonArray poolServers = config.getArray("pool-servers");

    if (poolServers == null) {
      return;
    }

    for (Object o : poolServers) {
      JsonObject locator = (JsonObject) o;
      String host = locator.getString("host");
      int port = locator.getInteger("port");
      factory.addPoolServer(host, port);
    }
  }


  private static void configurePDX(ClientCacheFactory factory, JsonObject config) {
    JsonObject pdxConfig = config.getObject("pdx");

    if (pdxConfig == null) {
      return;
    }

    String diskStoreName = pdxConfig.getString("disk-store-name");
    factory.setPdxDiskStore(diskStoreName);

    boolean ignore = pdxConfig.getBoolean("ignore-unread-fields", true);
    factory.setPdxIgnoreUnreadFields(ignore);

    boolean isPersistent = pdxConfig.getBoolean("persistent", true);
    factory.setPdxPersistent(isPersistent);

    boolean readSerialized = pdxConfig.getBoolean("read-serialized", true);
    factory.setPdxReadSerialized(readSerialized);

    String serializerClassName = pdxConfig.getString("pdx-serializer-class");
    PdxSerializer serializer = InstantiationUtils.instantiate(PdxSerializer.class, serializerClassName);
    factory.setPdxSerializer(serializer);
  }

}
