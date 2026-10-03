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

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import org.apache.nifi.controller.ArangoDBClientService;
import org.apache.nifi.controller.ArangoDBClientServiceImpl;
import org.apache.nifi.controller.ArangoDBLookupService;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArangoDBLookupServiceIT {
    private static final String DB = "lookup_tests";
    private static final String COL = "test_data";

    private TestRunner runner;
    private LookupService<Record> lookupService;
    private ArangoDB connection;

    @BeforeEach
    void setup() throws InitializationException {
        lookupService = new ArangoDBLookupService();
        ArangoDBClientService clientService = new ArangoDBClientServiceImpl();
        runner = TestRunners.newTestRunner(MockProcessor.class);
        runner.addControllerService("lookupService", lookupService);
        runner.addControllerService("clientService", clientService);
        ArangoDBTestContainer.configure(runner, clientService);
        runner.setProperty(clientService, ArangoDBClientServiceImpl.LOAD_BALANCING_STRATEGY,
                ArangoDBClientServiceImpl.LOAD_BALANCE_NONE);
        runner.setProperty(lookupService, ArangoDBLookupService.CLIENT_SERVICE, "clientService");
        runner.setProperty(lookupService, ArangoDBLookupService.DATABASE_NAME, DB);
        runner.setProperty(MockProcessor.LOOKUP_SERVICE, "lookupService");
        runner.enableControllerService(clientService);

        connection = clientService.getConnection();
        connection.db(DB).create();
        connection.db(DB).createCollection(COL);
    }

    @AfterEach
    void tearDown() {
        connection.db(DB).drop();
        connection.shutdown();
    }

    @Test
    void testSimpleNamedParameter() throws Exception {
        try (ArangoCursor<Object> cursor = connection.db(DB).query("""
                INSERT {
                    from: "e.goldstein",
                    to: "w.smith",
                    message: "My book is attached."
                } IN %s
            """.formatted(COL), Object.class)) {
            assertNotNull(cursor);
        }

        runner.setProperty(lookupService, ArangoDBLookupService.QUERY, """
            FOR message IN %s
                FILTER message.from == @is_from
            RETURN message
        """.formatted(COL));
        runner.setProperty(lookupService,
                lookupService.getPropertyDescriptor(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY.getName()),
                SchemaAccessUtils.INFER_SCHEMA);
        runner.enableControllerService(lookupService);

        Optional<Record> record = lookupService.lookup(Map.of("is_from", "e.goldstein"));
        assertTrue(record.isPresent());
    }
}
