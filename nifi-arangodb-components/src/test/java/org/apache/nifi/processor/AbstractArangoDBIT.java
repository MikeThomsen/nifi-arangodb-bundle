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

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import org.apache.nifi.controller.ArangoDBClientService;
import org.apache.nifi.controller.ArangoDBClientServiceImpl;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;

import java.util.Map;

abstract class AbstractArangoDBIT {
    static final String DATABASE = "nifi";
    static final String COLLECTION = "messages";

    TestRunner runner;
    ArangoDBClientService clientService;
    ArangoDB arangoDB;

    void setup(Class<? extends Processor> processorClass) throws InitializationException {
        clientService = new ArangoDBClientServiceImpl();
        runner = TestRunners.newTestRunner(processorClass);
        runner.addControllerService("clientService", clientService);
        ArangoDBTestContainer.configure(runner, clientService);
        runner.setProperty(clientService, ArangoDBClientServiceImpl.LOAD_BALANCING_STRATEGY,
                ArangoDBClientServiceImpl.LOAD_BALANCE_RANDOM);
        runner.setProperty(AbstractArangoDBProcessor.CLIENT_SERVICE, "clientService");
        runner.setProperty(AbstractArangoDBProcessor.DATABASE_NAME, DATABASE);
    }

    void setupTestDocuments() {
        arangoDB = clientService.getConnection();
        ArangoDatabase db = arangoDB.db(DATABASE);
        db.create();
        db.collection(COLLECTION).create();

        BaseDocument first = new BaseDocument("1");
        first.setProperties(Map.of("from", "john.smith", "to", "jane.doe", "message", "Hi!"));
        BaseDocument second = new BaseDocument("2");
        second.setProperties(Map.of("from", "jane.doe", "to", "john.smith", "message", "Bye!"));

        db.collection(COLLECTION).insertDocument(first);
        db.collection(COLLECTION).insertDocument(second);
    }

    @AfterEach
    void tearDown() {
        if (arangoDB != null) {
            arangoDB.db(DATABASE).drop();
            arangoDB.shutdown();
        }
    }
}
