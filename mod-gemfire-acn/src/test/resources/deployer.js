/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

load('vertx.js')

var config = {
  'cache-xml-file': 'src/test/resources/test-cache-client.xml',  // Use a default location?
  'properties-file': 'src/test/resources/client-test.properties', // Use a default location?
  'module-control-address': 'gemfire.client.control',
  properties: {},
  'pool-properties': {},
  'pool-locators': [
    {host: 'localhost', port: 40001}
  ],
// locators OR servers
//  'pool-servers': [
//    {host: 'localhost', port: 41001}
//  ],
  'pdx': {
    // TODO
  },
  'regions':[
    {name: 'testRegion1', shortcut: 'EMPTY'},
    {name: 'testRegion2', shortcut: 'EMPTY'}
  ],
  'subscriptions':[
    {region: 'testRegion1', policy:'DEFAULT', durable:false, 'receive-values':false, keys:['key1','key3','key7']},
    {region: 'testRegion2', policy:'DEFAULT', durable:false, 'receive-values':false, regex:'key[248]'}
  ],
  'continuous-queries':[
    {query: 'SELECT o FROM /testRegion1', name:'cq1', durable:false, address: 'test.gemfire.cq1'},
    {pool: 'pool1', query: 'SELECT o FROM /testRegion2', name:'cq2', durable:'false', address: 'test.gemfire.cq2'},
  ]
}

vertx.deployModule('vertx.gemfire-acn-v1.0', config, 1, function(id) {
  console.log('Deployed vertx.gemfire-acn-v1.0 ' + id);
});


function vertxStop() {
  //
  console.log("stopping...");
}
