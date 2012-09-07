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
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqListener;

public abstract class EventBusMappingCqEventListener implements CqListener, Declarable {

  @Override
  public void init(Properties properties) {
    // TODO Auto-generated method stub
  }

  @Override
  public void onEvent(CqEvent event) {
    Operation baseOperation = event.getBaseOperation();
    Operation queryOperation = event.getQueryOperation();

    String baseOp = "";
    String queryOp = "";

    if (baseOperation.isUpdate()) {
      baseOp = "Update";
    } else if (baseOperation.isCreate()) {
      baseOp = "Create";
    } else if (baseOperation.isDestroy()) {
      baseOp = "Destroy";
    } else if (baseOperation.isInvalidate()) {
      baseOp = "Invalidate";
    }

    if (queryOperation.isUpdate()) {
      queryOp = "Update";
    } else if (queryOperation.isCreate()) {
      queryOp = "Create";
    } else if (queryOperation.isDestroy()) {
      queryOp = "Destroy";
    }
    
    send(baseOp, queryOp);
  }

  @Override
  public void onError(CqEvent event) {
    //
  }

  @Override
  public void close() {
    //
  }

  protected abstract void send(String baseOp, String queryOp);

}
