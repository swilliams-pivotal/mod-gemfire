package org.vertx.mods.gemfire.support;

import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.Region;

public class RegionGetHandler implements Handler<Message<JsonObject>> {

  private Region<Object, Object> region;

  public RegionGetHandler(Region<Object, Object> region) {
    this.region = region;
  }

  @Override
  public void handle(Message<JsonObject> event) {

    Object key = event.body.getField("key");
    Object value = region.get(key);

    Map<String, Object> map = new HashMap<>();
    map.put("key", value);
    map.put("value", value);

    event.reply(new JsonObject(map));
  }

}
