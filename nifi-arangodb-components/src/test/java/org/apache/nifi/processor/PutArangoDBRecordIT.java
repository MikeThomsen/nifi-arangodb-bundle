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
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PutArangoDBRecordIT extends AbstractArangoDBIT {
    private MockRecordParser readerFactory;

    @BeforeEach
    void setup() throws InitializationException {
        readerFactory = new MockRecordParser();
        super.setup(PutArangoDBRecord.class);
        runner.addControllerService("recordReader", readerFactory);
        runner.setProperty(PutArangoDBRecord.COLLECTION_NAME, COLLECTION);
        runner.setProperty(PutArangoDBRecord.RECORD_READER, "recordReader");
        runner.setProperty(PutArangoDBRecord.KEY_RECORD_PATH, "/id");
        runner.enableControllerService(clientService);
        runner.enableControllerService(readerFactory);
        runner.assertValid();

        readerFactory.addSchemaField("id", RecordFieldType.INT);
        readerFactory.addSchemaField("message", RecordFieldType.STRING);
        readerFactory.addSchemaField("from", RecordFieldType.STRING);
        readerFactory.addSchemaField("to", RecordFieldType.STRING);

        readerFactory.addRecord(1, "Hello, world", "john.smith", "jane.doe");
        readerFactory.addRecord(2, "Goodbye!", "jane.doe", "john.smith");

        arangoDB = clientService.getConnection();
        arangoDB.db(DATABASE).create();
        arangoDB.db(DATABASE).createCollection(COLLECTION);
    }

    @Test
    void testBulkInsert() throws Exception {
        runner.enqueue("test");
        runner.run();

        runner.assertTransferCount(PutArangoDBRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutArangoDBRecord.REL_SUCCESS, 1);

        try (ArangoCursor<Long> cursor = arangoDB.db(DATABASE)
                .query("FOR message IN messages COLLECT WITH COUNT INTO cnt RETURN cnt", Long.class)) {
            assertEquals(2L, cursor.next());
        }
    }
}
