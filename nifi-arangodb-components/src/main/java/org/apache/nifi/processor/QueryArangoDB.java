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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({ "query", "arangodb" })
@CapabilityDescription("This is a generic query processor for ArangoDB. It is mainly intended for running aggregation queries such " +
        "as counts, deletes, updates, etc. It is not suitable for running large fetches of data because it keeps track of all of the output flowfiles " +
        "in an in-memory cache and will likely cause NiFi to run out of memory if pulling down a very large data set. Use QueryArangoDBRecord " +
        "for large fetches of records.")
public class QueryArangoDB extends AbstractArangoDBProcessor {
    public static final List<PropertyDescriptor> DESCRIPTORS = List.of(
        CLIENT_SERVICE, QUERY, DATABASE_NAME
    );

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS, REL_ORIGINAL, REL_FAILURE
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();

        ArangoDB connection = arangoDBClientService.getConnection();
        List<FlowFile> flowFiles = new ArrayList<>();
        try {
            try (ArangoCursor<Object> cursor = connection.db(databaseName).query(query, Object.class)) {
                while (cursor.hasNext()) {
                    flowFiles.add(writeOutput(asMap(cursor.next()), session, flowFile));
                }
            }

            if (flowFile != null) {
                session.transfer(flowFile, REL_ORIGINAL);
            }
        } catch (Exception ex) {
            getLogger().error("Query against database {} failed.", databaseName, ex);
            for (FlowFile ff : flowFiles) {
                session.remove(ff);
            }
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }
        } finally {
            connection.shutdown();
        }
    }

    /**
     * AQL can return documents, bare objects or scalars, so anything that is not already keyed gets wrapped under a
     * "result" key to keep the flowfile content a JSON object.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(Object value) {
        return switch (value) {
            case null -> Collections.singletonMap("result", null);
            case BaseDocument doc -> doc.getProperties();
            case Map<?, ?> map -> (Map<String, Object>) map;
            case Number number -> Map.<String, Object>of("result", number);
            default -> Map.<String, Object>of("result", value.toString());
        };
    }

    private FlowFile writeOutput(Map<String, Object> result, ProcessSession session, FlowFile parent) throws JsonProcessingException {
        String resultString = MAPPER.writeValueAsString(result);
        FlowFile resultFF = parent != null ? session.create(parent) : session.create();
        resultFF = session.write(resultFF, out -> out.write(resultString.getBytes()));
        session.transfer(resultFF, REL_SUCCESS);

        return resultFF;
    }
}
