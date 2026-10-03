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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryArangoDBIT extends AbstractArangoDBIT {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    void setup() throws InitializationException {
        super.setup(QueryArangoDB.class);
        runner.enableControllerService(clientService);
        super.setupTestDocuments();
    }

    private Map<String, Object> parse(MockFlowFile flowFile) throws Exception {
        return MAPPER.readValue(runner.getContentAsByteArray(flowFile), new TypeReference<>() { });
    }

    @Test
    void testCountQuery() throws Exception {
        runner.setProperty(QueryArangoDB.QUERY, "FOR message IN messages COLLECT WITH COUNT INTO cnt RETURN cnt");
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(QueryArangoDB.REL_SUCCESS, 1);
        runner.assertTransferCount(QueryArangoDB.REL_ORIGINAL, 0);

        Map<String, Object> parsed = parse(runner.getFlowFilesForRelationship(QueryArangoDB.REL_SUCCESS).get(0));
        assertEquals(1, parsed.size());
        assertEquals(2, ((Number) parsed.get("result")).intValue());
    }

    @Test
    void testMassInsert() {
        arangoDB.db(DATABASE).createCollection("users");
        runner.setProperty(QueryArangoDB.QUERY, "${query}");
        runner.enqueue("", Map.of("query", """
            FOR i IN 1..1000
              INSERT {
                id: 100000 + i,
                age: 18 + FLOOR(RAND() * 25),
                name: CONCAT('test', TO_STRING(i)),
                active: false,
                gender: i % 2 == 0 ? 'male' : 'female'
              } IN users
        """));
        runner.run();

        runner.assertTransferCount(QueryArangoDB.REL_FAILURE, 0);
        runner.assertTransferCount(QueryArangoDB.REL_SUCCESS, 0);
        runner.assertTransferCount(QueryArangoDB.REL_ORIGINAL, 1);
    }

    @Test
    void testRetrieve() throws Exception {
        runner.setProperty(QueryArangoDB.QUERY, "FOR message IN messages RETURN message");
        runner.run();

        runner.assertTransferCount(QueryArangoDB.REL_FAILURE, 0);
        runner.assertTransferCount(QueryArangoDB.REL_SUCCESS, 2);
        runner.assertTransferCount(QueryArangoDB.REL_ORIGINAL, 0);

        for (MockFlowFile flowFile : runner.getFlowFilesForRelationship(QueryArangoDB.REL_SUCCESS)) {
            Map<String, Object> parsed = parse(flowFile);
            assertTrue(parsed.size() >= 4, "Expected the document's own fields plus its system fields.");
            assertNotNull(parsed.get("to"));
            assertNotNull(parsed.get("from"));
            assertNotNull(parsed.get("message"));
        }
    }

    @Test
    void testBadQuery() {
        runner.setProperty(QueryArangoDB.QUERY, "DO nothing NOW");
        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(QueryArangoDB.REL_FAILURE, 1);
        runner.assertTransferCount(QueryArangoDB.REL_SUCCESS, 0);
        runner.assertTransferCount(QueryArangoDB.REL_ORIGINAL, 0);
    }

    @Test
    void testDelete() throws Exception {
        arangoDB.db(DATABASE).createCollection("users");
        try (ArangoCursor<Object> cursor = arangoDB.db(DATABASE)
                .query("INSERT { username: \"john.smith\" } IN users", Object.class)) {
            assertNotNull(cursor);
        }

        runner.setProperty(QueryArangoDB.QUERY, "FOR user IN users REMOVE user IN users");
        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(QueryArangoDB.REL_FAILURE, 0);
        runner.assertTransferCount(QueryArangoDB.REL_SUCCESS, 0);
        runner.assertTransferCount(QueryArangoDB.REL_ORIGINAL, 1);
    }
}
