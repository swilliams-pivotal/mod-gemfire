package org.vertx.mods.gemfire;

import static org.junit.Assert.*;

import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.test.TestVerticle;
import org.vertx.java.test.VertxTestBase;
import org.vertx.java.test.utils.QueueReplyHandler;
import org.vertx.java.test.junit.VertxJUnit4ClassRunner;

@RunWith(VertxJUnit4ClassRunner.class)
@TestVerticle(main="deployer.js")
public class GemFireClientCacheModTest extends VertxTestBase {

  @BeforeClass
  public static void prepare() throws Exception {
    // TODO setup system
  }

  @Before
  public void setUp() throws Exception {
    // TODO setup system
  }

  @Test
  public final void testCreateRegion() {
    String address = "gemfire.client.control";
    JsonObject message = new JsonObject();
    message.putString("create", "testRegion3");
    getEventBus().send(address, message);
  }

  @Test
  public final void testDestroyRegion() {
    String address = "gemfire.client.control";
    JsonObject message = new JsonObject();
    message.putString("destroy", "testRegion3"); // FIXME order required!
    getEventBus().send(address, message);
  }

  @Test
  public final void testGet() {
    LinkedBlockingQueue<JsonObject> queue = new LinkedBlockingQueue<JsonObject>();
    String address = "gemfire.client.testRegion1.get";

    JsonObject message = new JsonObject();
    message.putString("key", "key1");

    getEventBus().send(address, message, new QueueReplyHandler<JsonObject>(queue));
  }

  @Test
  public final void testPut() {

    LinkedBlockingQueue<JsonObject> queue = new LinkedBlockingQueue<JsonObject>();
    String address = "gemfire.client.testRegion1.put";

    JsonObject message = new JsonObject();
    message.putString("key", "key1");
    message.putString("value", "value1");

    getEventBus().send(address, message, new QueueReplyHandler<JsonObject>(queue));
  }

  @Test
  public final void testSubscribeKeys() {
    String address = "gemfire.client.control";

    JsonObject subscription = new JsonObject();
    subscription.putString("region", "testRegion1");
    subscription.putString("policy", "DEFAULT");
    subscription.putBoolean("durable", false);
    subscription.putBoolean("receive-values", false);
    JsonArray keys = new JsonArray();
    keys.addString("key1");
    keys.addString("key3");
    keys.addString("key5");
    subscription.putArray("keys", keys);

    JsonArray subscriptions = new JsonArray();
    subscriptions.addObject(subscription);

    JsonObject message = new JsonObject();
    message.putArray("subscriptions", subscriptions);

    getEventBus().send(address, message);
  }

  @Test
  public final void testUnsubscribeKey() {
    String address = "gemfire.client.control";

    JsonObject unsubscription = new JsonObject();
    unsubscription.putString("region", "testRegion1");
    JsonArray keys = new JsonArray();
    keys.addString("key1");
    keys.addString("key3");
    keys.addString("key5");
    unsubscription.putArray("keys", keys);

    JsonArray unsubscriptions = new JsonArray();
    unsubscriptions.addObject(unsubscription);

    JsonObject message = new JsonObject();
    message.putArray("unsubscriptions", unsubscriptions);

    getEventBus().send(address, message);
  }



  @Test
  public final void testSubscribeRegex() {
    String address = "gemfire.client.control";

    JsonObject subscription = new JsonObject();
    subscription.putString("region", "testRegion1");
    subscription.putString("policy", "DEFAULT");
    subscription.putBoolean("durable", false);
    subscription.putBoolean("receive-values", false);
    subscription.putString("regex", "key[248]");

    JsonArray subscriptions = new JsonArray();
    subscriptions.addObject(subscription);

    JsonObject message = new JsonObject();
    message.putArray("subscriptions", subscriptions);

    getEventBus().send(address, message);
  }

  @Test
  public final void testUnsubscribeRegex() {
    String address = "gemfire.client.control";

    JsonObject unsubscription = new JsonObject();
    unsubscription.putString("region", "testRegion1");
    unsubscription.putString("regex", "key[248]");

    JsonArray unsubscriptions = new JsonArray();
    unsubscriptions.addObject(unsubscription);

    JsonObject message = new JsonObject();
    message.putArray("unsubscriptions", unsubscriptions);

    getEventBus().send(address, message);
  }

  @Test
  public final void testContinuousQuery() {
    String replyAddress = String.format("test.gemfire.cq1.%d", System.currentTimeMillis());
    String address = "gemfire.client.control";

    JsonObject continuousQuery = new JsonObject();
    continuousQuery.putString("name", "cq1");
    continuousQuery.putString("query", "SELECT o FROM /testRegion2");
    continuousQuery.putBoolean("durable", false);
    continuousQuery.putString("address", replyAddress);

    JsonArray continuousQueries = new JsonArray();
    continuousQueries.addObject(continuousQuery);

    JsonObject message = new JsonObject();
    message.putArray("continuous-queries", continuousQueries);

    getEventBus().send(address, message);  }

  @After
  public void tearDown() throws Exception {
    // TODO shutdown system
  }

  @AfterClass
  public static void destroy() throws Exception {
    // TODO shutdown system
  }

}
