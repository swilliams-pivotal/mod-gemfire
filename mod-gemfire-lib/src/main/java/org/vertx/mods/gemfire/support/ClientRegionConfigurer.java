package org.vertx.mods.gemfire.support;

import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;

public class ClientRegionConfigurer {

  public static <K, V> Region<K, V> registerRegion(ClientCache cache, EventBus eventBus, JsonObject region) throws ClassNotFoundException {

    String shortcut = region.getString("shortcut");
    ClientRegionShortcut regionShortcut = ClientRegionShortcut.valueOf(shortcut.toUpperCase());
    ClientRegionFactory<K, V> factory;
    if (regionShortcut != null) {
      factory = cache.createClientRegionFactory(regionShortcut);
    }
    else {
      factory = cache.createClientRegionFactory(shortcut);
    }

//    factory.setCacheLoader(cacheLoader);
//    factory.setCacheWriter(cacheWriter);

    boolean cloningEnabled = region.getBoolean("cloning-enabled", false);
    factory.setCloningEnabled(cloningEnabled);

    int concurrencyLevel = region.getInteger("concurrency-level"); // required?
    factory.setConcurrencyLevel(concurrencyLevel);

//    factory.setCustomEntryIdleTimeout(customEntryIdleTimeout);
//    factory.setCustomEntryTimeToLive(customEntryTimeToLive);


    String diskStoreName = region.getString("diskStoreName");
    factory.setDiskStoreName(diskStoreName);

    boolean diskSynchronous = region.getBoolean("disk-synchronous", false);
    factory.setDiskSynchronous(diskSynchronous);

    ExpirationAttributes idleTimeout = new ExpirationAttributes();
    factory.setEntryIdleTimeout(idleTimeout);

    ExpirationAttributes timeToLive = new ExpirationAttributes();
    factory.setEntryTimeToLive(timeToLive);

    EvictionAttributes evictionAttributes = EvictionAttributes.createLRUEntryAttributes();
    factory.setEvictionAttributes(evictionAttributes);

    int initialCapacity = region.getInteger("initial-capacity");
    factory.setInitialCapacity(initialCapacity);

    String keyClassName = region.getString("key-classname");
    @SuppressWarnings("unchecked")
    Class<K> keyConstraint = (Class<K>) Class.forName(keyClassName);
    factory.setKeyConstraint(keyConstraint);

    float loadFactor = region.getNumber("load-factor").floatValue();
    factory.setLoadFactor(loadFactor);

    String poolName = region.getString("pool-name");
    factory.setPoolName(poolName);
    factory.setRegionIdleTimeout(idleTimeout);

    boolean statisticsEnabled = region.getBoolean("statistics-enabled", false);
    factory.setStatisticsEnabled(statisticsEnabled);

    String valueClassName = region.getString("value-classname");
    @SuppressWarnings("unchecked")
    Class<V> valueConstraint = (Class<V>) Class.forName(valueClassName);
    factory.setValueConstraint(valueConstraint);

    String name = region.getString("name");

    return factory.create(name);
  }
}
