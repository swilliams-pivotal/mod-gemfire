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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.mods.gemfire.support.ClientCacheConfigurer;
import org.vertx.mods.gemfire.support.ClientRegionConfigurer;
import org.vertx.mods.gemfire.support.CountDownLatchHandler;
import org.vertx.mods.gemfire.support.EventBusCacheListener;
import org.vertx.mods.gemfire.support.RegionGetHandler;
import org.vertx.mods.gemfire.support.RegionPutHandler;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;

public class GemFireClientCacheMod extends BusModBase implements Handler<Message<JsonObject>> {

  private Set<String> handlers = new HashSet<String>();

  private ClientCache clientCache;

  @Override
  public void start() {
    super.start();

    JsonObject config = getContainer().getConfig();

    this.clientCache = ClientCacheConfigurer.configure(vertx.fileSystem(), config);

    JsonArray regions = config.getArray("regions");
    registerRegions(regions);

    JsonArray continuousQueries = config.getArray("continuous-queries");
    registerContinuousQueries(continuousQueries);

    JsonArray subscriptions = config.getArray("subscriptions");
    registerSubscriptions(subscriptions);

    boolean durable = false;
    if (durable) {
      clientCache.readyForEvents();
    }

    String handler = eb.registerHandler("vertx.gemfire.client.control", this);
    handlers.add(handler);
  }

  @Override
  public void stop() throws Exception {
    CountDownLatch latch = new CountDownLatch(handlers.size());

    for (String handler : handlers) {
      eb.unregisterHandler(handler, new CountDownLatchHandler<Void>(latch));
    }

    latch.await(15000L, TimeUnit.MILLISECONDS);

    if (clientCache != null) {
      clientCache.close();
    }

    super.stop();
  }

  private void registerRegions(JsonArray regions) {
    if (regions == null) {
      return;
    }

    for (Object o : regions) {
      try {
        JsonObject regionConf = (JsonObject) o;
        Region<Object, Object> region = ClientRegionConfigurer
          .registerRegion(clientCache, eb, regionConf);

        String address = regionConf.getString("address");
        if (address == null) {
          address = String.format("gemfire.client.%s", region.getName());
        }

        String putHandler = eb.registerHandler(String.format("%s.put", address), new RegionPutHandler(region));
        handlers.add(putHandler);

        String getHandler = eb.registerHandler(String.format("%s.get", address), new RegionGetHandler(region));
        handlers.add(getHandler);

        if (address != null) {
          region.getAttributesMutator()
            .addCacheListener(new EventBusCacheListener(eb, String.format("%s.events", address)));
        }

      } catch (Exception e) {
        throw new RuntimeException("TODO: TEMPORARY EXCEPTION TYPE", e);
      }
    }
  }

  private void registerContinuousQueries(JsonArray continuousQueries) {
    if (continuousQueries == null ) {
      return;
    }

    for (Object o : continuousQueries) {
      JsonObject continuousQuery = (JsonObject) o;
      registerContinuousQuery(continuousQuery);
    }
  }

  private String registerContinuousQuery(JsonObject continuousQuery) {

    final String pool = continuousQuery.getString("pool");
    QueryService queryService;
    if (pool != null) {
      queryService = clientCache.getQueryService(pool);
    }
    else {
      queryService = clientCache.getQueryService();
    }

    String query = continuousQuery.getString("query");

    final String queryName = continuousQuery.getString("name");
    final boolean isDurable = continuousQuery.getBoolean("durable", false);

    try {
      CqAttributesFactory cqf = new CqAttributesFactory();
      final String returnAddress = continuousQuery.getString("address");
      cqf.addCqListener(new EventBusCqListener(eb, returnAddress));

      CqAttributes cqa = cqf.create();
      CqQuery cq;
      if (queryName != null) {
        cq = queryService.newCq(queryName, query, cqa, isDurable);
      }
      else {
        cq = queryService.newCq(query, cqa, isDurable);
      }

      cq.execute();

      return cq.getName();

    } catch (NullPointerException e) {
      throw new ContinuousQueryRegistrationException(e);
    } catch (QueryInvalidException e) {
      throw new ContinuousQueryRegistrationException(e);
    } catch (CqExistsException e) {
      throw new ContinuousQueryRegistrationException(e);
    } catch (CqException e) {
      throw new ContinuousQueryRegistrationException(e);
    } catch (CqClosedException e) {
      throw new ContinuousQueryRegistrationException(e);
    } catch (RegionNotFoundException e) {
      throw new ContinuousQueryRegistrationException(e);
    }
  }

  private void registerSubscriptions(JsonArray subscriptions) {
    if (subscriptions == null || subscriptions.size() == 0) {
      return;
    }

    for (Object o : subscriptions) {
      JsonObject subscription = (JsonObject) o;
      subscribe(subscription);
    }
  }

  private void subscribe(JsonObject subscription) {
    String regionName = subscription.getString("region");

    if (regionName != null) {
      Region<Object, Object> region = clientCache.getRegion(regionName);

      String policyName = subscription.getString("policy", "DEFAULT");
      InterestResultPolicyMap policyMap = InterestResultPolicyMap.valueOf(policyName.toUpperCase());
      boolean isDurable = subscription.getBoolean("durable", false);
      boolean receiveValues = subscription.getBoolean("receive-values", false);

      JsonArray keyArray = subscription.getArray("keys", new JsonArray());

      List<String> keys = new ArrayList<>();
      for (Object o : keyArray) {
        String key = (String) o;
        keys.add(key);
      }

      if (keys.size() > 0) {
        region.registerInterest(keys, policyMap.policy(), isDurable, receiveValues);
      }

      String regex = subscription.getString("regex");
      if (regex != null) {
        region.registerInterestRegex(regex, policyMap.policy(), isDurable, receiveValues);
      }
    }
  }

  private void unsubscribe(JsonObject unsubscription) {
    String regionName = unsubscription.getString("region");

    if (regionName != null) {
      Region<Object, Object> region = clientCache.getRegion(regionName);
      JsonArray keyArray = unsubscription.getArray("keys", new JsonArray());

      List<String> keys = new ArrayList<>();
      for (Object o : keyArray) {
        String key = (String) o;
        keys.add(key);
      }

      if (keys.size() > 0) {
        region.unregisterInterest(keys);
      }

      String regex = unsubscription.getString("regex");
      if (regex != null) {
        region.unregisterInterestRegex(regex);
      }
    }
  }

  @Override
  public void handle(final Message<JsonObject> event) {
    vertx.runOnLoop(new Handler<Void>() {
      @Override
      public void handle(Void unused) {
        deferredHandle(event);
      }
    });
  }

  private void deferredHandle(final Message<JsonObject> event) {

    JsonObject body = event.body;

    JsonArray subscriptions = body.getArray("subscriptions");
    if (subscriptions != null) {
      registerSubscriptions(subscriptions);
    }

    JsonArray unsubscriptions = body.getArray("unsubscriptions");
    if (unsubscriptions != null) {
      for (Object o : unsubscriptions) {
        JsonObject unsubscription = (JsonObject) o;
        unsubscribe(unsubscription);
      }
    }

    JsonArray continuousQueries = body.getArray("continuous-queries");
    if (continuousQueries != null) {
      for (Object o : continuousQueries) {
        JsonObject continuousQuery = (JsonObject) o;
        registerContinuousQuery(continuousQuery);
      }
    }

    event.reply();
  }

}
