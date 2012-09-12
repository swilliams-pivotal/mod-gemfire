package org.vertx.mods.gemfire.support;

import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;

public class RegionConfigurer {

  public static <K, V> Region<K, V> registerRegion(Cache cache, EventBus eventBus, JsonObject region) throws ClassNotFoundException {

    String shortcut = region.getString("shortcut");
    RegionShortcut regionShortcut = RegionShortcut.valueOf(shortcut.toUpperCase());
    RegionFactory<K, V> factory;
    if (regionShortcut != null) {
      factory = cache.createRegionFactory(regionShortcut);
    }
    else {
      factory = cache.createRegionFactory();
    }

//    factory.setCacheLoader(cacheLoader);
//    factory.setCacheWriter(cacheWriter);

    boolean cloningEnabled = region.getBoolean("cloning-enabled", false);
    factory.setCloningEnabled(cloningEnabled);

    int concurrencyLevel = region.getInteger("concurrency-level"); // required?
    factory.setConcurrencyLevel(concurrencyLevel);

//    factory.setCustomEntryIdleTimeout(customEntryIdleTimeout);
//    factory.setCustomEntryTimeToLive(customEntryTimeToLive);

    byte dataPolicyOrdinal = 0;
    DataPolicy dataPolicy = DataPolicy.fromOrdinal(dataPolicyOrdinal);
    factory.setDataPolicy(dataPolicy);

    String diskStoreName = region.getString("disk-store-name");
    factory.setDiskStoreName(diskStoreName);

    boolean diskSynchronous = region.getBoolean("disk-synchronous", false);
    factory.setDiskSynchronous(diskSynchronous);

    boolean asyncConflation = region.getBoolean("async-conflation", false);
    factory.setEnableAsyncConflation(asyncConflation);

    boolean enableGateway = region.getBoolean("enable-gateway", false);
    factory.setEnableGateway(enableGateway);

    boolean subscriptionConflation = region.getBoolean("subscription-conflation", false);
    factory.setEnableSubscriptionConflation(subscriptionConflation);

    ExpirationAttributes idleTimeout = new ExpirationAttributes();
    factory.setEntryIdleTimeout(idleTimeout);

    ExpirationAttributes timeToLive = new ExpirationAttributes();
    factory.setEntryTimeToLive(timeToLive);

    EvictionAttributes evictionAttributes = EvictionAttributes.createLRUEntryAttributes();
    factory.setEvictionAttributes(evictionAttributes);

    String gatewayHubId = region.getString("gateway-hub");
    factory.setGatewayHubId(gatewayHubId);

    boolean ignoreJTA = region.getBoolean("ignore-jta", false);
    factory.setIgnoreJTA(ignoreJTA);

    boolean indexMaintenanceSynchronous = region.getBoolean("index-maintenance-synchronous", false);
    factory.setIndexMaintenanceSynchronous(indexMaintenanceSynchronous);

    String keyClassName = region.getString("key-classname");
    @SuppressWarnings("unchecked")
    Class<K> keyConstraint = (Class<K>) Class.forName(keyClassName);
    factory.setKeyConstraint(keyConstraint);

    float loadFactor = region.getNumber("load-factor").floatValue();
    factory.setLoadFactor(loadFactor);

    boolean isLockGrantor = region.getBoolean("lock-grantor", false);
    factory.setLockGrantor(isLockGrantor);
    MembershipAttributes membershipAttributes = new MembershipAttributes();
    factory.setMembershipAttributes(membershipAttributes);

    boolean multicastEnabled = region.getBoolean("multicast-enabled", false);
    factory.setMulticastEnabled(multicastEnabled);

    PartitionAttributesFactory<Object, Object> partitionAttributesFactory = new PartitionAttributesFactory<>();
    PartitionAttributes<Object, Object> partitionAttributes = partitionAttributesFactory.create();
    factory.setPartitionAttributes(partitionAttributes);

    String poolName = region.getString("pool-name");
    factory.setPoolName(poolName);
    factory.setRegionIdleTimeout(idleTimeout);

    String scopeName = region.getString("scope");
    Scope scopeType = Scope.fromString(scopeName);
    factory.setScope(scopeType);

    boolean statisticsEnabled = region.getBoolean("statistics-enabled", false);
    factory.setStatisticsEnabled(statisticsEnabled);
    SubscriptionAttributes subscriptionAttributes = new SubscriptionAttributes();
    factory.setSubscriptionAttributes(subscriptionAttributes);

    String valueClassName = region.getString("value-classname");
    @SuppressWarnings("unchecked")
    Class<V> valueConstraint = (Class<V>) Class.forName(valueClassName);
    factory.setValueConstraint(valueConstraint);

    String name = region.getString("name");

    return factory.create(name);
  }
}
