package org.vertx.mods.gemfire.support;

public class InstantiationUtils {

  public static final <T> T instantiate(Class<T> clazz, String implementationName) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    return instantiate(clazz, implementationName, loader);
  }

  public static final <T> T instantiate(Class<T> clazz, String implementationName, ClassLoader loader) {
    return instantiate(clazz, implementationName, true, loader);
  }

  public static final <T> T instantiate(Class<T> clazz, String implementationName, boolean initialize, ClassLoader loader) {

    try {
      Class<?> rawClass = Class.forName(implementationName, initialize, loader);
      Class<? extends T> implClass = rawClass.asSubclass(clazz);
      return implClass.newInstance();

    } catch (ClassNotFoundException | InstantiationException
        | IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
  }

}
