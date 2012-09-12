package org.vertx.mods.gemfire.support;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

import org.vertx.java.core.json.JsonObject;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.pdx.PdxSerializer;

public class CacheConfigurer {

  public static Cache configure(JsonObject config) {
    CacheFactory factory = new CacheFactory();

    configurePropertiesFile(factory, config);

    configureProperties(factory, config);

    configurePDX(factory, config);

    Cache cache = factory.create();

    String cacheXmlFile = config.getString("cache-xml-file", "cache.xml");
    System.out.printf("cacheXmlFile: %s%n", cacheXmlFile);

    try (InputStream is = new FileInputStream(new File(cacheXmlFile))) {
      cache.loadCacheXml(is);
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return cache;
  }

  private static void configurePDX(CacheFactory factory, JsonObject config) {

    JsonObject pdxConfig = config.getObject("pdx");
    if (pdxConfig == null) {
      return;
    }

    String diskStoreName = pdxConfig.getString("disk-store-name");
    factory.setPdxDiskStore(diskStoreName);

    boolean ignore = pdxConfig.getBoolean("ignore-unread-fields", true);
    factory.setPdxIgnoreUnreadFields(ignore);

    boolean isPersistent = pdxConfig.getBoolean("persistent", true);
    factory.setPdxPersistent(isPersistent);

    boolean readSerialized = pdxConfig.getBoolean("read-serialized", true);
    factory.setPdxReadSerialized(readSerialized);

    String serializerClassName = pdxConfig.getString("pdx-serializer-class");
    PdxSerializer serializer = InstantiationUtils.instantiate(PdxSerializer.class, serializerClassName);
    factory.setPdxSerializer(serializer);
  }

  private static void configurePropertiesFile(CacheFactory factory, JsonObject config) {

    if (config == null) {
      return;
    }

    String propertiesFile = config.getString("properties-file", "gemfire.properties");
    System.out.printf("propertiesFile: %s%n", propertiesFile);

    if (propertiesFile == null) {
      return;
    }

    Properties properties = new Properties();

    try (InputStream is = new FileInputStream(new File(propertiesFile))) {
      if (propertiesFile.endsWith(".xml")) {
        properties.loadFromXML(is);
      }
      else {
        properties.load(is);
      }

      for (String name : properties.stringPropertyNames()) {
        String value = properties.getProperty(name);
        factory.set(name, value);
      }

    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private static void configureProperties(CacheFactory factory, JsonObject config) {

    Set<String> fieldNames = config.getFieldNames();
    if (!fieldNames.contains("properties")) {
      return;
    }

    JsonObject propertiesConf = config.getObject("properties");

    if (propertiesConf.getFieldNames().size() == 0) {
      return;
    }

    Set<String> propNames = propertiesConf.getFieldNames();
    for (String name : propNames) {
      String value = propertiesConf.getString(name);
      factory.set(name, value);
    }
  }

}
