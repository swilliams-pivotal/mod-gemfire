package org.vertx.mods.gemfire.support;

import java.util.Properties;

import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

public class EventBusCacheListener extends CacheListenerAdapter<Object, Object> implements Declarable {

  private final EventBus eventBus;

  private final String address;

  private Properties properties;

  public EventBusCacheListener(EventBus eventBus, String address) {
    this.eventBus = eventBus;
    this.address = address;
  }

  @Override
  public void afterCreate(EntryEvent<Object, Object> event) {
    JsonObject message = new JsonObject();
    message.putString("key-type", event.getKey().getClass().getName());
    message.putString("new-value-type", event.getNewValue().getClass().getName());
    if (event.isOldValueAvailable()) {
      message.putString("old-value-type", event.getOldValue().getClass().getName());
    }

    eventBus.publish(address, message);
  }

  @Override
  public void afterDestroy(EntryEvent<Object, Object> event) {
    JsonObject message = new JsonObject();
    message.putString("key-type", event.getKey().getClass().getName());
    message.putString("new-value-type", event.getNewValue().getClass().getName());
    if (event.isOldValueAvailable()) {
      message.putString("old-value-type", event.getOldValue().getClass().getName());
    }

    eventBus.publish(address, message);
  }

  @Override
  public void afterInvalidate(EntryEvent<Object, Object> event) {
    JsonObject message = new JsonObject();
    message.putString("key-type", event.getKey().getClass().getName());
    message.putString("new-value-type", event.getNewValue().getClass().getName());
    if (event.isOldValueAvailable()) {
      message.putString("old-value-type", event.getOldValue().getClass().getName());
    }

    eventBus.publish(address, message);
  }

  @Override
  public void afterUpdate(EntryEvent<Object, Object> event) {
    JsonObject message = new JsonObject();
    message.putString("key-type", event.getKey().getClass().getName());
    message.putString("new-value-type", event.getNewValue().getClass().getName());
    if (event.isOldValueAvailable()) {
      message.putString("old-value-type", event.getOldValue().getClass().getName());
    }

    eventBus.publish(address, message);
  }

  @Override
  public void afterRegionClear(RegionEvent<Object, Object> event) {
    JsonObject message = new JsonObject();

    eventBus.publish(address, message);
  }

  @Override
  public void afterRegionCreate(RegionEvent<Object, Object> event) {
    JsonObject message = new JsonObject();

    eventBus.publish(address, message);
  }

  @Override
  public void afterRegionDestroy(RegionEvent<Object, Object> event) {
    JsonObject message = new JsonObject();

    eventBus.publish(address, message);
  }

  @Override
  public void afterRegionInvalidate(RegionEvent<Object, Object> event) {
    JsonObject message = new JsonObject();

    eventBus.publish(address, message);
  }

  @Override
  public void afterRegionLive(RegionEvent<Object, Object> event) {
    JsonObject message = new JsonObject();

    eventBus.publish(address, message);
  }

  @Override
  public void init(Properties properties) {
    this.properties = properties;

    for (String name : this.properties.stringPropertyNames()) {
      System.out.printf("%s=%s%n", name, this.properties.getProperty(name));
    }
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

}
