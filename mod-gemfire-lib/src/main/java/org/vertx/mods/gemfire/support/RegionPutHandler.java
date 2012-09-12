package org.vertx.mods.gemfire.support;

import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.Region;

public class RegionPutHandler implements Handler<Message<JsonObject>> {

  private Region<Object, Object> region;

  public RegionPutHandler(Region<Object, Object> region) {
    this.region = region;
  }

  @Override
  public void handle(Message<JsonObject> event) {

    Object key = event.body.getField("key");
    Object value = event.body.getField("value");

    Object replaced = region.put(key, value);

    Map<String, Object> map = new HashMap<>();
    map.put("key", value);
    map.put("replaced", replaced);

    event.reply(new JsonObject(map));
  }

}
