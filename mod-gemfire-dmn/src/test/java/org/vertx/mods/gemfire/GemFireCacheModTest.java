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
public class GemFireCacheModTest {

  @BeforeClass
  public static void prepare() throws Exception {
    // TODO setup system
  }

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public final void testPut() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testGet() {
    fail("Not yet implemented"); // TODO
  }

  @After
  public void tearDown() throws Exception {
  }

  @AfterClass
  public static void destroy() throws Exception {
    // TODO shutdown system
  }

}
