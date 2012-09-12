package org.vertx.mods.gemfire;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.vertx.java.test.TestVerticle;
import org.vertx.java.test.junit.VertxJUnit4ClassRunner;

@RunWith(VertxJUnit4ClassRunner.class)
@TestVerticle(main="deployer.js")
public class GemFireGatewayModTest {

  @BeforeClass
  public static void prepare() throws Exception {
    // TODO setup system
  }

  @Before
  public void setUp() throws Exception {
    // TODO start gemfire system + gateway
  }

  @Test
  public final void testReceiveMessage() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testReceiveMessageGroup() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testSendMessage() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testSendMessageGroup() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testAddGateway() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testRemoveGateway() {
    fail("Not yet implemented"); // TODO
  }

  @After
  public void tearDown() throws Exception {
    // TODO stop gemfire system + gateway
  }

  @AfterClass
  public static void destroy() throws Exception {
    // TODO shutdown system
  }
}
