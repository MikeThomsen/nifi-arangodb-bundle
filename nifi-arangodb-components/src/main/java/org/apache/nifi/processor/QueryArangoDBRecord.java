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
import com.arangodb.entity.BaseDocument;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({ "query", "arangodb", "record" })
@CapabilityDescription("This processor is intended to be used for fetching large volumes of data from ArangoDB. It uses " +
        "the NiFi Record API to provide the ability serialize result sets in a clean and consistent manner. For deletes, updates " +
        "and aggregation queries, see QueryArangoDB.")
public class QueryArangoDBRecord extends AbstractArangoDBProcessor {
    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("arango-query-record-writer")
        .displayName("Record Writer")
        .description("The record writer to use for writing the result set.")
        .required(true)
        .identifiesControllerService(RecordSetWriterFactory.class)
        .addValidator(Validator.VALID)
        .build();

    public static final List<PropertyDescriptor> DESCRIPTORS = List.of(
        CLIENT_SERVICE, QUERY, RECORD_WRITER, DATABASE_NAME
    );

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS, REL_FAILURE, REL_ORIGINAL
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private volatile RecordSetWriterFactory writerFactory;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
        writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        FlowFile flowFile = session.get();
        FlowFile output = flowFile != null ? session.create(flowFile) : session.create();
        Map<String, String> attributes = flowFile != null ? flowFile.getAttributes() : Map.of();
        ArangoDB connection = arangoDBClientService.getConnection();

        try (OutputStream os = session.write(output)) {
            String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();
            String dbName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();
            RecordSchema schema = writerFactory.getSchema(attributes, null);

            try (RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, os, attributes);
                 ArangoCursor<BaseDocument> results = connection.db(dbName).query(query, BaseDocument.class)) {
                writer.beginRecordSet();
                while (results.hasNext()) {
                    BaseDocument document = results.next();
                    Record record = new MapRecord(schema, document.getProperties());
                    writer.write(record);
                }
                writer.finishRecordSet();
            }

            session.transfer(output, REL_SUCCESS);
            if (flowFile != null) {
                session.transfer(flowFile, REL_ORIGINAL);
            }
        } catch (Exception ex) {
            getLogger().error("Query against database {} failed.", context.getProperty(DATABASE_NAME).getValue(), ex);
            session.remove(output);
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }
        } finally {
            connection.shutdown();
        }
    }
}
