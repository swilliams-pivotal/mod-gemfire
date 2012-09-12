package org.vertx.mods.gemfire;

import java.util.List;
import java.util.Properties;

import org.vertx.java.core.eventbus.EventBus;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.util.GatewayEvent;
import com.gemstone.gemfire.cache.util.GatewayEventListener;

public class EventBusGatewayEventListener implements GatewayEventListener, Declarable {

  private final EventBus eventBus;

  private final String address;

  public EventBusGatewayEventListener(EventBus eventBus, String address) {
    this.eventBus = eventBus;
    this.address = address;
  }

  @Override
  public void init(Properties properties) {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean processEvents(List<GatewayEvent> events) {

    int count = 0;
 
    for (GatewayEvent event : events) {
      try {
        processEvent(event);
        count++;
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    return (count == events.size());
  }

  public void processEvent(GatewayEvent event) throws Exception {
    byte[] message = event.getSerializedValue();
    eventBus.publish(address, message);
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }
}