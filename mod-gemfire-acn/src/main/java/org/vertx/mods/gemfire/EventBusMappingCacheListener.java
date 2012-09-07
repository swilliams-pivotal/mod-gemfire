/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.vertx.mods.gemfire;

import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

public class EventBusMappingCacheListener<K,V> extends CacheListenerAdapter<K,V> implements Declarable {

  @Override
  public void init(Properties props) {
    //
  }

  @Override
  public void afterCreate(EntryEvent<K, V> event) {
    // TODO Auto-generated method stub
    super.afterCreate(event);
  }

  @Override
  public void afterDestroy(EntryEvent<K, V> event) {
    // TODO Auto-generated method stub
    super.afterDestroy(event);
  }

  @Override
  public void afterInvalidate(EntryEvent<K, V> event) {
    // TODO Auto-generated method stub
    super.afterInvalidate(event);
  }

  @Override
  public void afterUpdate(EntryEvent<K, V> event) {
    // TODO Auto-generated method stub
    super.afterUpdate(event);
  }

}
