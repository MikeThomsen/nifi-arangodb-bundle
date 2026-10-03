/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processor;

import org.apache.nifi.arango.common.ArangoClientConfiguration;
import org.apache.nifi.controller.ArangoDBClientService;

public abstract class AbstractArangoDBProcessor extends AbstractProcessor implements ArangoClientConfiguration {
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All successful flowfiles go to this relationship.")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("All failed flowfiles go to this relationship.")
        .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("When the operation succeeeds, the original input flowfile will go this relationship.")
        .build();

    protected volatile ArangoDBClientService arangoDBClientService;

    public void onScheduled(ProcessContext context) {
        arangoDBClientService = context.getProperty(CLIENT_SERVICE).asControllerService(ArangoDBClientService.class);
    }
}
