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

  private ClientCache cache;

  @Override
  public void start() {
    super.start();

    JsonObject config = getContainer().getConfig();

    ClientCacheConfigurer configurer = new ClientCacheConfigurer(config);

    this.cache = configurer.create();

    JsonArray continuousQueries = config.getArray("continuousQueries");
    registerContinuousQueries(continuousQueries);

    JsonArray subscriptions = config.getArray("subscriptions");
    registerSubscriptions(subscriptions);

    String handler = eb.registerHandler("vertx.gemfire.bridge", this);
    handlers.add(handler);
  }

  private void registerSubscriptions(JsonArray subscriptions) {
    if (subscriptions == null) {
      return;
    }

    for (Object o : subscriptions) {
      JsonObject subscription = (JsonObject) o;
      subscribe(subscription);
    }
  }

  @Override
  public void stop() throws Exception {
    CountDownLatch latch = new CountDownLatch(handlers.size());

    for (String handler : handlers) {
      eb.unregisterHandler(handler, new CountDownLatchHandler(latch));
    }

    latch.await(15000L, TimeUnit.MILLISECONDS);

    if (cache != null) {
      cache.close();
    }

    super.stop();
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
      queryService = cache.getQueryService(pool);
    }
    else {
      queryService = cache.getQueryService();
    }

    String query = continuousQuery.getString("query");

    final String returnAddress = continuousQuery.getString("returnAddress");
    CqAttributesFactory cqf = new CqAttributesFactory();
    cqf.addCqListener(new EventBusMappingCqEventListener() {

      @Override
      protected void send(String baseOp, String queryOp) {
        JsonObject message = new JsonObject();
        message.putString("baseOp", baseOp);
        message.putString("queryOp", queryOp);
        eb.send(returnAddress, message);
      }
    });

    final String queryName = continuousQuery.getString("queryName");
    final boolean isDurable = continuousQuery.getBoolean("isDurable", false);

    try {
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

  private void subscribe(JsonObject subscription) {
    String regionName = subscription.getString("region");

    if (regionName != null) {
      Region<Object, Object> region = cache.getRegion(regionName);

      String policyName = subscription.getString("policy", "DEFAULT");
      InterestResultPolicyMap policyMap = InterestResultPolicyMap.valueOf(policyName.toUpperCase());
      boolean isDurable = subscription.getBoolean("isDurable", false);
      boolean receiveValues = subscription.getBoolean("receiveValues", false);

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
      Region<Object, Object> region = cache.getRegion(regionName);
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

        JsonObject body = event.body;
        String type = body.getString("type");

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

        if ("continuousQueries".endsWith(type)) {
          JsonArray continuousQueries = body.getArray("continuousQueries");
          for (Object o : continuousQueries) {
            JsonObject continuousQuery = (JsonObject) o;
            registerContinuousQuery(continuousQuery);
          }
        }

        event.reply();
      }
      
    });
  }

}
