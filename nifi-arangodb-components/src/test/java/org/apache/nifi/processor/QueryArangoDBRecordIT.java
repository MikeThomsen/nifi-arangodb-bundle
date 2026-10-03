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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryArangoDBRecordIT extends AbstractArangoDBIT {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    void setup() throws InitializationException {
        super.setup(QueryArangoDBRecord.class);

        RecordSchema schema = new SimpleRecordSchema(List.of(
            new RecordField("id", RecordFieldType.LONG.getDataType()),
            new RecordField("message", RecordFieldType.STRING.getDataType()),
            new RecordField("from", RecordFieldType.STRING.getDataType()),
            new RecordField("to", RecordFieldType.STRING.getDataType())
        ), SchemaIdentifier.builder().name("message").build());

        MockSchemaRegistry schemaRegistry = new MockSchemaRegistry();
        schemaRegistry.addSchema("message", schema);
        JsonRecordSetWriter writer = new JsonRecordSetWriter();

        runner.addControllerService("schemaRegistry", schemaRegistry);
        runner.addControllerService("writer", writer);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "schemaRegistry");
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        runner.setProperty(QueryArangoDBRecord.RECORD_WRITER, "writer");
        runner.setProperty(QueryArangoDBRecord.QUERY, "FOR message IN messages RETURN message");
        runner.enableControllerService(schemaRegistry);
        runner.enableControllerService(writer);
        runner.enableControllerService(clientService);

        super.setupTestDocuments();
    }

    @Test
    void testRetrieveRecords() throws Exception {
        runner.enqueue("", Map.of("schema.name", "message"));
        runner.run();

        runner.assertTransferCount(QueryArangoDBRecord.REL_FAILURE, 0);
        runner.assertTransferCount(QueryArangoDBRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(QueryArangoDBRecord.REL_ORIGINAL, 1);

        byte[] content = runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QueryArangoDBRecord.REL_SUCCESS).get(0));
        List<Map<String, Object>> parsed = MAPPER.readValue(content, new TypeReference<>() { });
        assertEquals(2, parsed.size());
    }
}
